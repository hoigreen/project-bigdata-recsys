from pyspark.sql import SparkSession, functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator

import os
import shutil
import time
import json
from datetime import datetime
import psycopg2

# Support both local (localhost:5433) and container (postgres:5432) execution
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get(
    "PG_PORT", "5433" if PG_HOST == "localhost" else "5432")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "postgres")
PG_DB = os.environ.get("PG_DB", "recsys")

# Artifacts directory for saving models
ARTIFACT_DIR = os.environ.get("ARTIFACT_DIR", "/opt/bigdata-recsys/models")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Model versioning
MODEL_VERSION = datetime.now().strftime("%Y%m%d_%H%M%S")


def get_pg_conn():
    """Get PostgreSQL connection for logging metrics."""
    return psycopg2.connect(
        host=PG_HOST,
        port=int(PG_PORT),
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )


def log_als_training_metrics(metrics: dict):
    """Log ALS training metrics to PostgreSQL for tracking."""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()

        # Deactivate previous active ALS models
        cur.execute("""
            UPDATE model_training_history 
            SET is_active = FALSE 
            WHERE model_name = %s AND is_active = TRUE
        """, (metrics['model_name'],))

        # Insert new training record
        cur.execute("""
            INSERT INTO model_training_history (
                model_name, model_version, training_samples, validation_samples,
                train_auc, valid_auc, train_logloss, valid_logloss,
                train_accuracy, valid_accuracy, num_features, num_courses, num_users,
                hyperparameters, artifact_path, training_duration_seconds,
                data_snapshot_timestamp, is_active, notes
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s
            )
        """, (
            metrics['model_name'],
            metrics['model_version'],
            metrics['training_samples'],
            metrics['validation_samples'],
            metrics.get('train_rmse'),  # Using RMSE in AUC field for ALS
            metrics.get('test_rmse'),   # Using test RMSE in valid_auc field
            metrics.get('train_mae'),   # Using MAE in logloss field
            metrics.get('test_mae'),    # Using test MAE in valid_logloss field
            None,  # accuracy not applicable for ALS
            None,
            metrics['rank'],  # Using rank as num_features
            metrics['num_items'],
            metrics['num_users'],
            json.dumps(metrics['hyperparameters']),
            metrics['artifact_path'],
            metrics['training_duration_seconds'],
            metrics['data_snapshot_timestamp'],
            metrics.get('notes', f'ALS training at {datetime.now()}')
        ))

        conn.commit()
        print(
            f"✅ ALS training metrics logged to database (version: {metrics['model_version']})")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ Warning: Could not log ALS training metrics: {e}")


def main():
    training_start_time = time.time()
    data_snapshot_timestamp = datetime.now()
    # Khởi tạo Spark
    spark = (
        SparkSession.builder
        .appName("ALS_Train")
        # Spark tự tải PostgreSQL JDBC driver
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("🔗 Đang đọc bảng interactions từ Postgres...")
    interactions = spark.read.jdbc(
        JDBC_URL, "interactions", properties=JDBC_PROPS)

    print(f"📊 Số bản ghi interactions: {interactions.count()}")

    # Bỏ các dòng không có truth
    df = interactions.na.drop(subset=["truth"])

    # Index user_id / course_id -> int cho ALS
    print("🔢 Đang index user_id và course_id...")

    user_indexer = StringIndexer(
        inputCol="user_id",
        outputCol="user_idx",
        handleInvalid="skip",  # bỏ row lỗi
    )

    item_indexer = StringIndexer(
        inputCol="course_id",
        outputCol="item_idx",
        handleInvalid="skip",
    )

    user_model = user_indexer.fit(df)
    df = user_model.transform(df)

    item_model = item_indexer.fit(df)
    df = item_model.transform(df)

    df = (
        df.withColumn("user_idx", F.col("user_idx").cast("int"))
          .withColumn("item_idx", F.col("item_idx").cast("int"))
    )

    print("✂️ Split train / test...")
    train, test = df.randomSplit([0.8, 0.2], seed=42)

    print("🧠 Train ALS...")
    als = ALS(
        userCol="user_idx",
        itemCol="item_idx",
        ratingCol="truth",
        implicitPrefs=True,         # vì truth là kiểu “interaction/label”
        coldStartStrategy="drop",
        nonnegative=True,
        rank=50,
        maxIter=10,
        regParam=0.01,
    )

    model = als.fit(train)

    print("📏 Evaluate trên test...")
    predictions = model.transform(test)
    train_predictions = model.transform(train)

    # RMSE Evaluator
    rmse_evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="truth",
        predictionCol="prediction",
    )

    # MAE Evaluator
    mae_evaluator = RegressionEvaluator(
        metricName="mae",
        labelCol="truth",
        predictionCol="prediction",
    )

    test_rmse = rmse_evaluator.evaluate(predictions)
    train_rmse = rmse_evaluator.evaluate(train_predictions)
    test_mae = mae_evaluator.evaluate(predictions)
    train_mae = mae_evaluator.evaluate(train_predictions)

    print(f"✅ Train RMSE = {train_rmse:.4f}")
    print(f"✅ Test RMSE = {test_rmse:.4f}")
    print(f"✅ Train MAE = {train_mae:.4f}")
    print(f"✅ Test MAE = {test_mae:.4f}")

    # Lưu model ALS vào thư mục artifacts (optional, skip on permission errors)
    als_model_path = os.path.join(ARTIFACT_DIR, "als_model")
    print(f"💾 Đang lưu ALS model vào {als_model_path} ...")
    try:
        # Clean up existing model directory to avoid permission issues
        if os.path.exists(als_model_path):
            shutil.rmtree(als_model_path, ignore_errors=True)
        model.save(als_model_path)
        print("✅ Lưu model thành công!")
    except Exception as e:
        print(
            f"⚠️ Không thể lưu model file (sẽ chỉ export sang Postgres): {e}")

    # ===== Export user_factors / item_factors sang Postgres =====
    print("📦 Đang export user_factors và item_factors...")

    # Map từ index -> id gốc
    user_map = df.select("user_id", "user_idx").distinct()
    item_map = df.select("course_id", "item_idx").distinct()

    # user_factors: id (user_idx), features (array<double>)
    user_factors = (
        model.userFactors
        .join(user_map, model.userFactors.id == user_map.user_idx, "inner")
        .select(
            F.col("user_id").cast("bigint").alias("user_id"),
            F.to_json("features").alias("factors"),
        )
    )

    # item_factors: id (item_idx), features
    item_factors = (
        model.itemFactors
        .join(item_map, model.itemFactors.id == item_map.item_idx, "inner")
        .select(
            F.col("course_id").alias("course_id"),
            F.to_json("features").alias("factors"),
        )
    )

    print("📝 Ghi als_user_factors vào Postgres (overwrite)...")
    (
        user_factors.write
        .mode("overwrite")
        .jdbc(JDBC_URL, "als_user_factors", properties=JDBC_PROPS)
    )

    print("📝 Ghi als_item_factors vào Postgres (overwrite)...")
    (
        item_factors.write
        .mode("overwrite")
        .jdbc(JDBC_URL, "als_item_factors", properties=JDBC_PROPS)
    )

    # Calculate training duration
    training_duration = time.time() - training_start_time

    # Get statistics
    num_users = df.select("user_id").distinct().count()
    num_items = df.select("course_id").distinct().count()

    # Log training metrics to database
    als_hyperparams = {
        'rank': 50,
        'maxIter': 10,
        'regParam': 0.01,
        'implicitPrefs': True,
        'coldStartStrategy': 'drop',
        'nonnegative': True
    }

    als_metrics = {
        'model_name': 'spark_als',
        'model_version': MODEL_VERSION,
        'training_samples': train.count(),
        'validation_samples': test.count(),
        'train_rmse': train_rmse,
        'test_rmse': test_rmse,
        'train_mae': train_mae,
        'test_mae': test_mae,
        'rank': 50,
        'num_items': num_items,
        'num_users': num_users,
        'hyperparameters': als_hyperparams,
        'artifact_path': als_model_path,
        'training_duration_seconds': training_duration,
        'data_snapshot_timestamp': data_snapshot_timestamp,
        'notes': f'ALS training - Train RMSE: {train_rmse:.4f}, Test RMSE: {test_rmse:.4f}'
    }

    log_als_training_metrics(als_metrics)

    print("\n" + "="*60)
    print("   ALS TRAINING SUMMARY")
    print("="*60)
    print(f"   Model Version:     {MODEL_VERSION}")
    print(f"   Training Duration: {training_duration:.2f} seconds")
    print(f"   Training Samples:  {train.count()}")
    print(f"   Test Samples:      {test.count()}")
    print(f"   Train RMSE:        {train_rmse:.4f}")
    print(f"   Test RMSE:         {test_rmse:.4f}")
    print(f"   Total Users:       {num_users}")
    print(f"   Total Items:       {num_items}")
    print("="*60)

    print("\n🎉 Xong pipeline ALS!")
    spark.stop()


if __name__ == "__main__":
    main()
