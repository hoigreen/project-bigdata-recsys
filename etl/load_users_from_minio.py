from minio import Minio
import csv
import psycopg2
from psycopg2.extras import execute_values
import time
import os

# ===== Config =====
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "mooc-data"
MINIO_OBJECT = "user_info.csv"      # Đảm bảo đúng tên file trên MinIO

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", 5432))  # Internal container port
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "postgres")
PG_DB = os.environ.get("PG_DB", "recsys")

LOCAL_TMP_PATH = "/tmp/user_info_tmp.csv"   # file tạm lưu về local
BATCH_SIZE = 5000
# ==================

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )


def parse_birth_year(raw):
    if raw is None or raw == "":
        return None
    try:
        return int(float(raw))   # "1997.0" -> 1997
    except ValueError:
        return None


def main():
    start_time = time.time()
    print("🔗 Đang tải file từ MinIO về local...")

    # Tạo folder tmp nếu chưa có
    os.makedirs("/tmp", exist_ok=True)

    # Tải file từ MinIO về local (overwrite nếu đã tồn tại)
    minio_client.fget_object(
        MINIO_BUCKET,
        MINIO_OBJECT,
        LOCAL_TMP_PATH
    )
    print(f"✅ Đã tải xong: {LOCAL_TMP_PATH}")

    print("🔗 Đang kết nối Postgres & đọc CSV...")
    conn = get_pg_conn()
    cur = conn.cursor()

    total_rows = 0
    batch = []

    with open(LOCAL_TMP_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        print("🧾 Header CSV:", reader.fieldnames)

        for i, row in enumerate(reader, start=1):
            try:
                user_id = int(row["user_id"])
            except (ValueError, TypeError, KeyError):
                print(
                    f"⚠️  Bỏ qua dòng {i}: user_id không hợp lệ -> {row.get('user_id')}")
                continue

            gender = row.get("gender") or None
            education = row.get("education") or None
            birth_year = parse_birth_year(row.get("birth"))

            batch.append((user_id, gender, education, birth_year))

            if len(batch) >= BATCH_SIZE:
                execute_values(cur, """
                    INSERT INTO users (user_id, gender, education, birth_year)
                    VALUES %s
                    ON CONFLICT (user_id) DO NOTHING;
                """, batch)
                conn.commit()

                total_rows += len(batch)
                batch.clear()

                elapsed = time.time() - start_time
                print(
                    f"✅ Đã insert ~{total_rows} dòng (elapsed: {elapsed:.1f}s)")

    # Insert nốt batch cuối
    if batch:
        execute_values(cur, """
            INSERT INTO users (user_id, gender, education, birth_year)
            VALUES %s
            ON CONFLICT (user_id) DO NOTHING;
        """, batch)
        conn.commit()
        total_rows += len(batch)

    cur.close()
    conn.close()

    elapsed = time.time() - start_time
    print(f"🎉 Hoàn thành! Tổng insert ~{total_rows} dòng trong {elapsed:.1f}s")


if __name__ == "__main__":
    main()
