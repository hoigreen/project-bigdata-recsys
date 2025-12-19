"""
DAG for MOOC Recommendation System Pipeline:
1. Load data from MinIO -> Postgres (ETL)
2. Train LightGBM model
3. Train Spark ALS model
4. Run real-time streaming (Producer/Consumer) after training completes
5. Periodic model retraining with fresh data from DB
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

# Streaming duration (configurable)
STREAMING_DURATION = 600  # seconds

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

# Spark environment
SPARK_ENV = {
    **ENV_VARS,
    "PYSPARK_PYTHON": "/usr/local/bin/python3",
    "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
    "ARTIFACT_DIR": "/opt/bigdata-recsys/models",
}

default_args = {
    "owner": "recsys",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# =============================================================================
# DAG 1: recsys_pipeline - Main Full Pipeline (ETL -> Train -> Stream)
# =============================================================================
with DAG(
    dag_id="recsys_pipeline",
    default_args=default_args,
    description="Complete pipeline: ETL -> Train LGBM & ALS -> Run Streaming",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
    max_active_runs=1,
    tags=["recsys", "mooc", "full-pipeline"],
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
        env={**ENV_VARS, "ENABLE_MODEL_VERSIONING": "true"},
    )

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
        env=SPARK_ENV,
        trigger_rule="all_success",
    )

    # ===== Sync point after training =====
    training_complete = DummyOperator(
        task_id="training_complete",
        trigger_rule="all_success",
    )

    # ===== Streaming Tasks =====
    run_producer = BashOperator(
        task_id="run_producer",
        bash_command=(
            f"cd {CORE_LOGIC_DIR} && "
            f"timeout {STREAMING_DURATION} {PYTHON_BIN} run_producer.py || true"
        ),
        env=ENV_VARS,
    )

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
    load_interactions >> [train_lgbm, spark_als_train]

    # Wait for both training tasks to complete
    [train_lgbm, spark_als_train] >> training_complete

    # Streaming: Run producer and consumer in parallel after training
    training_complete >> [run_producer, run_consumer]


# =============================================================================
# DAG 2: recsys_periodic_retrain - Scheduled Model Retraining
# =============================================================================
with DAG(
    dag_id="recsys_periodic_retrain",
    default_args={
        **default_args,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    description="Periodic retraining of LightGBM model with fresh data from PostgreSQL",
    start_date=datetime(2024, 1, 1),
    # Schedule: Run daily at 2 AM (configurable)
    # Options: '@daily', '@hourly', '@weekly', '0 2 * * *' (cron)
    schedule_interval="0 2 * * *",  # Daily at 2:00 AM
    catchup=False,
    max_active_runs=1,
    tags=["recsys", "retrain", "lgbm", "scheduled"],
) as retrain_dag:

    # Environment variables for retraining
    RETRAIN_ENV_VARS = {
        **ENV_VARS,
        "ENABLE_MODEL_VERSIONING": "true",
        "KEEP_MODEL_VERSIONS": "5",  # Keep last 5 versions
    }

    # Check if there's data to train on
    check_data_freshness = BashOperator(
        task_id="check_data_freshness",
        bash_command="""
            cd /opt/bigdata-recsys && python3 -c "
import psycopg2
import os

conn = psycopg2.connect(
    host=os.environ.get('PG_HOST', 'postgres'),
    port=int(os.environ.get('PG_PORT', 5432)),
    user=os.environ.get('PG_USER', 'postgres'),
    password=os.environ.get('PG_PASSWORD', 'postgres'),
    dbname=os.environ.get('PG_DB', 'recsys')
)
cur = conn.cursor()

# Check if there are interactions in the database
cur.execute('SELECT COUNT(*) FROM interactions')
total_count = cur.fetchone()[0]

# Check for recent data (added in last 24 hours)
cur.execute('''
    SELECT COUNT(*) FROM interactions 
    WHERE created_at > NOW() - INTERVAL \\'24 hours\\'
''')
recent_count = cur.fetchone()[0]

conn.close()

if total_count == 0:
    print('❌ No data in interactions table. Skipping retraining.')
    exit(1)

print(f'✅ Data check passed: {total_count} total interactions, {recent_count} recent')
"
        """,
        env=RETRAIN_ENV_VARS,
    )

    # Retrain LightGBM model with fresh data
    retrain_lgbm = BashOperator(
        task_id="retrain_lgbm",
        bash_command=f"cd {CORE_LOGIC_DIR} && {PYTHON_BIN} train_module.py",
        env=RETRAIN_ENV_VARS,
    )

    # Notify on completion
    retrain_complete = BashOperator(
        task_id="retrain_complete",
        bash_command="""
            echo "✅ LightGBM model retraining completed at $(date)"
            echo "   Model artifacts updated at: $ARTIFACT_DIR"
        """,
        env=RETRAIN_ENV_VARS,
    )

    # Task dependencies
    check_data_freshness >> retrain_lgbm >> retrain_complete


# =============================================================================
# DAG 3: recsys_retrain_on_demand - Manual Retraining with Parameters
# =============================================================================
with DAG(
    dag_id="recsys_retrain_on_demand",
    default_args=default_args,
    description="On-demand model retraining with configurable hyperparameters",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["recsys", "retrain", "manual", "lgbm"],
    params={
        "learning_rate": 0.05,
        "num_leaves": 31,
        "num_boost_round": 500,
        "early_stopping": 50,
        "notes": "Manual retraining",
    },
) as on_demand_dag:

    # Retrain with configurable parameters
    retrain_on_demand_lgbm = BashOperator(
        task_id="retrain_lgbm",
        bash_command=f"""
            cd {CORE_LOGIC_DIR} && \\
            LGBM_LEARNING_RATE={{{{ params.learning_rate }}}} \\
            LGBM_NUM_LEAVES={{{{ params.num_leaves }}}} \\
            LGBM_NUM_BOOST_ROUND={{{{ params.num_boost_round }}}} \\
            LGBM_EARLY_STOPPING={{{{ params.early_stopping }}}} \\
            {PYTHON_BIN} train_module.py
        """,
        env={
            **ENV_VARS,
            "ENABLE_MODEL_VERSIONING": "true",
            "KEEP_MODEL_VERSIONS": "10",
        },
    )

    retrain_on_demand_complete = BashOperator(
        task_id="training_complete",
        bash_command="""
            echo "✅ On-demand retraining completed"
            echo "   Notes: {{ params.notes }}"
        """,
        env=ENV_VARS,
    )

    retrain_on_demand_lgbm >> retrain_on_demand_complete
