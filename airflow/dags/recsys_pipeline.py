"""
DAG for MOOC Recommendation System Pipeline:
1. Load data from MinIO -> Postgres (ETL)
2. Train LightGBM model
3. Train Spark ALS model
4. Run real-time streaming (Producer/Consumer) after training completes
"""
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

BASE_DIR = "/opt/bigdata-recsys"
CORE_LOGIC_DIR = os.path.join(BASE_DIR, "core-logic")
PYTHON_BIN = "/usr/local/bin/python"

# Shared artifacts directory (mounted volume)
ARTIFACT_DIR = "/opt/recsys-artifacts"

# Environment variables for services
ENV_VARS = {
    "PG_HOST": "postgres",
    "PG_PORT": "5432",
    "PG_USER": "postgres",
    "PG_PASSWORD": "postgres",
    "PG_DB": "recsys",
    "KAFKA_BOOTSTRAP": "kafka:29092",
    "KAFKA_TOPIC": "mooc-events",
    "PYTHONPATH": "/home/airflow/.local/lib/python3.8/site-packages",
    "ARTIFACT_DIR": ARTIFACT_DIR,
}

default_args = {
    "owner": "recsys",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="recsys_pipeline",
    default_args=default_args,
    description="MOOC Recommendation System: ETL -> Train -> Inference",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
    max_active_runs=1,
    tags=["recsys", "mooc", "recommendation"],
) as dag:

    # ===== ETL Tasks =====
    load_users = BashOperator(
        task_id="load_users_from_minio",
        bash_command=f"cd {BASE_DIR} && {PYTHON_BIN} etl/load_users_from_minio.py",
        env=ENV_VARS,
    )

    load_interactions = BashOperator(
        task_id="load_interactions_from_minio",
        bash_command=f"cd {BASE_DIR} && {PYTHON_BIN} etl/load_interactions_from_minio.py",
        env=ENV_VARS,
    )

    # ===== Training Tasks =====
    train_lgbm = BashOperator(
        task_id="train_lgbm",
        bash_command=f"cd {CORE_LOGIC_DIR} && {PYTHON_BIN} train_module.py",
        env=ENV_VARS,
    )

    # Spark ALS training - submit to Spark cluster
    spark_env = {
        **ENV_VARS,
        "PYSPARK_PYTHON": "/usr/local/bin/python3",
        "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
        "PYTHONPATH": "/home/airflow/.local/lib/python3.8/site-packages",
        "ARTIFACT_DIR": "/opt/bigdata-recsys/models",
    }

    spark_als_train = BashOperator(
        task_id="spark_als_train",
        bash_command=(
            f"cd {BASE_DIR} && "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--packages org.postgresql:postgresql:42.7.3 "
            "--conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2 "
            "--conf spark.pyspark.python=/usr/local/bin/python3 "
            "--conf spark.pyspark.driver.python=/usr/local/bin/python3 "
            "--conf spark.executor.memory=1g "
            "--conf spark.driver.memory=1g "
            "spark_jobs/batch_als_train.py"
        ),
        env=spark_env,
        trigger_rule="all_success",
    )

    # ===== Sync point after training =====
    training_complete = DummyOperator(
        task_id="training_complete",
        trigger_rule="all_success",
    )

    # ===== Streaming Tasks (after training) =====
    # Producer: Simulate user events (runs for configurable duration)
    STREAMING_DURATION = 120  # seconds - adjust as needed

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command=(
            f"cd {CORE_LOGIC_DIR} && "
            f"timeout {STREAMING_DURATION} {PYTHON_BIN} run_producer.py || true"
        ),
        env=ENV_VARS,
    )

    # Consumer: Process events and generate recommendations
    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command=(
            f"cd {CORE_LOGIC_DIR} && "
            f"timeout {STREAMING_DURATION} {PYTHON_BIN} run_consumer.py || true"
        ),
        env=ENV_VARS,
    )

    # ===== Pipeline Flow =====
    # ETL: Load users first, then interactions
    load_users >> load_interactions

    # Training: After ETL, train both models in parallel
    load_interactions >> train_lgbm
    load_interactions >> spark_als_train

    # Wait for both training tasks to complete
    [train_lgbm, spark_als_train] >> training_complete

    # Streaming: Run producer and consumer in parallel after training
    training_complete >> [run_producer, run_consumer]


# ===== Separate DAG for Streaming Only (Manual) =====
# Use this DAG when you want to run streaming without re-training
with DAG(
    dag_id="recsys_streaming_only",
    default_args=default_args,
    description="Run Kafka Producer/Consumer only (assumes models are already trained)",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["recsys", "streaming", "kafka"],
) as streaming_dag:

    # Configurable streaming duration
    STREAMING_ONLY_DURATION = 300  # 5 minutes

    # Producer: Simulate user events
    streaming_producer = BashOperator(
        task_id="run_producer",
        bash_command=(
            f"cd {CORE_LOGIC_DIR} && "
            f"timeout {STREAMING_ONLY_DURATION} {PYTHON_BIN} run_producer.py || true"
        ),
        env=ENV_VARS,
    )

    # Consumer: Process events and generate recommendations
    streaming_consumer = BashOperator(
        task_id="run_consumer",
        bash_command=(
            f"cd {CORE_LOGIC_DIR} && "
            f"timeout {STREAMING_ONLY_DURATION} {PYTHON_BIN} run_consumer.py || true"
        ),
        env=ENV_VARS,
    )

    # Run producer and consumer in parallel
    [streaming_producer, streaming_consumer]


# ===== Full Pipeline: ETL -> Train -> Streaming =====
with DAG(
    dag_id="recsys_full_pipeline",
    default_args=default_args,
    description="Complete pipeline: ETL -> Train LGBM & ALS -> Run Streaming",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
    max_active_runs=1,
    tags=["recsys", "mooc", "full-pipeline"],
) as full_dag:

    FULL_STREAMING_DURATION = 120  # 2 minutes for full pipeline

    # ETL Tasks
    full_load_users = BashOperator(
        task_id="load_users_from_minio",
        bash_command=f"cd {BASE_DIR} && {PYTHON_BIN} etl/load_users_from_minio.py",
        env=ENV_VARS,
    )

    full_load_interactions = BashOperator(
        task_id="load_interactions_from_minio",
        bash_command=f"cd {BASE_DIR} && {PYTHON_BIN} etl/load_interactions_from_minio.py",
        env=ENV_VARS,
    )

    # Training Tasks
    full_train_lgbm = BashOperator(
        task_id="train_lgbm",
        bash_command=f"cd {CORE_LOGIC_DIR} && {PYTHON_BIN} train_module.py",
        env=ENV_VARS,
    )

    full_spark_env = {
        **ENV_VARS,
        "PYSPARK_PYTHON": "/usr/local/bin/python3",
        "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
        "ARTIFACT_DIR": "/opt/bigdata-recsys/models",
    }

    full_spark_als_train = BashOperator(
        task_id="spark_als_train",
        bash_command=(
            f"cd {BASE_DIR} && "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--packages org.postgresql:postgresql:42.7.3 "
            "--conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2 "
            "--conf spark.pyspark.python=/usr/local/bin/python3 "
            "--conf spark.pyspark.driver.python=/usr/local/bin/python3 "
            "--conf spark.executor.memory=1g "
            "--conf spark.driver.memory=1g "
            "spark_jobs/batch_als_train.py"
        ),
        env=full_spark_env,
    )

    full_training_complete = DummyOperator(
        task_id="training_complete",
        trigger_rule="all_success",
    )

    # Streaming Tasks
    full_run_producer = BashOperator(
        task_id="run_producer",
        bash_command=(
            f"cd {CORE_LOGIC_DIR} && "
            f"timeout {FULL_STREAMING_DURATION} {PYTHON_BIN} run_producer.py || true"
        ),
        env=ENV_VARS,
    )

    full_run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command=(
            f"cd {CORE_LOGIC_DIR} && "
            f"timeout {FULL_STREAMING_DURATION} {PYTHON_BIN} run_consumer.py || true"
        ),
        env=ENV_VARS,
    )

    # Pipeline Flow
    full_load_users >> full_load_interactions
    full_load_interactions >> [full_train_lgbm, full_spark_als_train]
    [full_train_lgbm, full_spark_als_train] >> full_training_complete
    full_training_complete >> [full_run_producer, full_run_consumer]
