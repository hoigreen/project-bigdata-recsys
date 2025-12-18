from minio import Minio
import csv
import psycopg2
from psycopg2.extras import execute_values
import time
import os
import sys

# Tăng giới hạn kích thước 1 field (mặc định ~128KB)
csv.field_size_limit(sys.maxsize)

# ===== Config =====
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "mooc-data"
MINIO_OBJECT = "train.csv"          # đảm bảo đúng tên file trên MinIO

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", 5432))  # Internal container port
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "postgres")
PG_DB = os.environ.get("PG_DB", "recsys")

LOCAL_TMP_PATH = "/tmp/train_tmp.csv"
BATCH_SIZE = 5000
# ==================

# Các cột hành động + feature đúng schema LGBM
ACTION_COLS = [
    "action_click_about", "action_click_courseware", "action_click_forum", "action_click_info",
    "action_click_progress", "action_close_courseware", "action_close_forum", "action_create_comment",
    "action_create_thread", "action_delete_comment", "action_delete_thread", "action_load_video",
    "action_pause_video", "action_play_video", "action_problem_check", "action_problem_check_correct",
    "action_problem_check_incorrect", "action_problem_get", "action_problem_save", "action_reset_problem",
    "action_seek_video", "action_stop_video", "unique_session_count", "avg_nActions_per_session",
]

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


def parse_float(raw):
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def parse_int(raw):
    if raw is None or raw == "":
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def main():
    start_time = time.time()
    print("🔗 Đang tải train.csv từ MinIO về local...")

    os.makedirs("/tmp", exist_ok=True)

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
                user_id = int(row["username"])  # username = user_id
            except (ValueError, TypeError, KeyError):
                print(
                    f"⚠️  Bỏ qua dòng {i}: username không hợp lệ -> {row.get('username')}")
                continue

            course_id = row.get("course_id")
            if not course_id:
                print(f"⚠️  Bỏ qua dòng {i}: course_id trống")
                continue

            truth = parse_int(row.get("truth"))
            event_ts = parse_int(row.get("timestamp"))

            action_values = [parse_float(row.get(col)) for col in ACTION_COLS]

            batch.append(
                (user_id, course_id, truth, *action_values, event_ts)
            )

            if len(batch) >= BATCH_SIZE:
                execute_values(cur, f"""
                    INSERT INTO interactions (
                        user_id, course_id, truth,
                        {", ".join(ACTION_COLS)},
                        event_ts
                    )
                    VALUES %s
                    ON CONFLICT (user_id, course_id) DO NOTHING;
                """, batch)
                conn.commit()

                total_rows += len(batch)
                batch.clear()

                elapsed = time.time() - start_time
                print(
                    f"✅ Đã insert ~{total_rows} dòng (elapsed: {elapsed:.1f}s)")

    # Insert nốt batch cuối
    if batch:
        execute_values(cur, f"""
            INSERT INTO interactions (
                user_id, course_id, truth,
                {", ".join(ACTION_COLS)},
                event_ts
            )
            VALUES %s
            ON CONFLICT (user_id, course_id) DO NOTHING;
        """, batch)
        conn.commit()
        total_rows += len(batch)

    cur.close()
    conn.close()

    elapsed = time.time() - start_time
    print(f"🎉 Hoàn thành! Tổng insert ~{total_rows} dòng trong {elapsed:.1f}s")


if __name__ == "__main__":
    main()
