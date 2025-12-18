from pyspark.sql import SparkSession, functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator


import os
import shutil

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


def main():
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

    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="truth",
        predictionCol="prediction",
    )

    rmse = evaluator.evaluate(predictions)
    print(f"✅ RMSE trên test = {rmse:.4f}")

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

    print("🎉 Xong pipeline ALS!")
    spark.stop()


if __name__ == "__main__":
    main()
