# train_module.py
# Train LightGBM model from Postgres data (loaded via ETL from MinIO)
import pandas as pd
import numpy as np
import lightgbm as lgb
import os
import sys
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, log_loss
from sklearn.preprocessing import LabelEncoder
from collections import Counter
import itertools
import pickle
import json
import psycopg2

# ===========================
# CẤU HÌNH
# ===========================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Postgres config (internal container network)
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", 5432))
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "postgres")
PG_DB = os.environ.get("PG_DB", "recsys")

# Output artifacts directory (writable location)
ARTIFACT_DIR = os.environ.get("ARTIFACT_DIR", "/tmp/recsys-artifacts")
os.makedirs(ARTIFACT_DIR, exist_ok=True)

MODEL_PATH = os.path.join(ARTIFACT_DIR, "lgb_model_binary.txt")
HISTORY_PATH = os.path.join(ARTIFACT_DIR, "user_history_snapshot.csv")
ENCODER_PATH = os.path.join(ARTIFACT_DIR, "course_encoder.pkl")
KNOWLEDGE_PATH = os.path.join(ARTIFACT_DIR, "knowledge_base.pkl")
IDS_MAPPING_PATH = os.path.join(ARTIFACT_DIR, "ids_mapping.json")


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )


# Feature columns
FEATURE_COLS = [
    'course_id_int',
    'action_click_about', 'action_click_courseware', 'action_click_forum', 'action_click_info',
    'action_click_progress', 'action_close_courseware', 'action_close_forum', 'action_create_comment',
    'action_create_thread', 'action_delete_comment', 'action_delete_thread', 'action_load_video',
    'action_pause_video', 'action_play_video', 'action_problem_check', 'action_problem_check_correct',
    'action_problem_check_incorrect', 'action_problem_get', 'action_problem_save', 'action_reset_problem',
    'action_seek_video', 'action_stop_video', 'unique_session_count', 'avg_nActions_per_session'
]

ACTION_COLS = [c for c in FEATURE_COLS if c != 'course_id_int']


def main():
    print("⏳ [1/6] Reading data from Postgres...")

    try:
        conn = get_pg_conn()

        # Load all interactions
        interactions_query = "SELECT * FROM interactions"
        train_df = pd.read_sql(interactions_query, conn)

        # Load user info
        users_query = "SELECT user_id, gender, education, birth_year FROM users"
        user_info = pd.read_sql(users_query, conn)

        conn.close()

        print(f"📋 Available columns: {list(train_df.columns)}")

        # Add missing feature columns with default value 0
        for col in ACTION_COLS:
            if col not in train_df.columns:
                print(f"⚠️  Adding missing column: {col}")
                train_df[col] = 0.0

    except Exception as e:
        print(f"❌ Database connection error: {e}")
        print("   Make sure Postgres is running and ETL has been completed.")
        sys.exit(1)

    if train_df.empty:
        print("❌ No data found in interactions table!")
        print("   Run ETL scripts first to load data from MinIO.")
        sys.exit(1)

    print(f"✅ Loaded {len(train_df)} interactions and {len(user_info)} users")

    # Convert IDs to string for consistency
    train_df['user_id'] = train_df['user_id'].astype(str)
    user_info['user_id'] = user_info['user_id'].astype(str)

    # Merge user info
    train_df = train_df.merge(user_info, on='user_id', how='left')

    # ===========================
    # XỬ LÝ MÃ HÓA (ENCODING)
    # ===========================
    print("⚙️  Encoding Course IDs...")
    all_courses = train_df['course_id'].astype(str).unique()
    le = LabelEncoder()
    le.fit(all_courses)

    # Save encoder
    with open(ENCODER_PATH, 'wb') as f:
        pickle.dump(le, f)

    # Apply encoding
    train_df['course_id_int'] = le.transform(train_df['course_id'].astype(str))

    # ===========================
    # CHUẨN BỊ DỮ LIỆU
    # ===========================
    X = train_df[FEATURE_COLS].fillna(0)
    y = train_df['truth'].fillna(-1)
    mask = y.isin([0, 1])
    X = X[mask]
    y = y[mask]

    if len(X) == 0:
        print("❌ No valid training samples with truth labels!")
        sys.exit(1)

    print(
        f"✅ Training samples: {len(X)} (Pass: {(y == 0).sum()}, Fail: {(y == 1).sum()})")

    # ===========================
    # [2/6] HUẤN LUYỆN MODEL
    # ===========================
    print("💪 [2/6] Training LightGBM Model...")
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y)

    lgb_train = lgb.Dataset(X_train, label=y_train,
                            categorical_feature=['course_id_int'])
    lgb_val = lgb.Dataset(X_val, label=y_val, reference=lgb_train)

    params = {
        'objective': 'binary',
        'metric': ['auc', 'binary_logloss'],
        'learning_rate': 0.05,
        'num_leaves': 31,
        'seed': 42,
        'verbose': -1
    }

    clf = lgb.train(
        params, lgb_train, num_boost_round=500,
        valid_sets=[lgb_train, lgb_val],
        callbacks=[lgb.early_stopping(50), lgb.log_evaluation(0)]
    )
    clf.save_model(MODEL_PATH)
    print(f"✅ Model saved at: {MODEL_PATH}")

    # ===========================
    # [3/6] ĐÁNH GIÁ
    # ===========================
    print("\n" + "="*60)
    print("   [3/6] PERFORMANCE REPORT")
    print("="*60)

    def calculate_metrics(name, X_data, y_data, model, threshold=0.6):
        if len(y_data) == 0:
            return None
        probs = model.predict(X_data)
        preds = [1 if p >= threshold else 0 for p in probs]
        return {
            'Dataset': name,
            'Samples': len(y_data),
            'AUC': roc_auc_score(y_data, probs),
            'Logloss': log_loss(y_data, probs),
            'Accuracy (%)': round(accuracy_score(y_data, preds) * 100, 2)
        }

    metrics_list = []
    metrics_list.append(calculate_metrics('TRAIN', X_train, y_train, clf))
    metrics_list.append(calculate_metrics('VALID', X_val, y_val, clf))

    results_df = pd.DataFrame(
        [m for m in metrics_list if m is not None]).set_index('Dataset')
    print(results_df)
    print("="*60 + "\n")

    # ===========================
    # [4/6] TẠO SNAPSHOT LỊCH SỬ
    # ===========================
    print("💾 [4/6] Exporting history snapshot...")
    history_cols = FEATURE_COLS + ['user_id', 'course_id', 'truth']

    # Rename for compatibility with consumer
    full_history = train_df[history_cols].copy()
    full_history = full_history.rename(columns={'user_id': 'username'})

    # Aggregate by user-course
    agg_dict = {col: 'sum' for col in ACTION_COLS}
    agg_dict['course_id_int'] = 'first'
    agg_dict['truth'] = 'max'

    history_snapshot = full_history.groupby(
        ['username', 'course_id']).agg(agg_dict).reset_index()
    history_snapshot.to_csv(HISTORY_PATH, index=False)
    print(f"✅ History snapshot saved: {len(history_snapshot)} records")

    # ===========================
    # [5/6] TẠO KNOWLEDGE BASE
    # ===========================
    print("\n🔍 [5/6] Building Knowledge Base...")

    # 1. Course co-occurrence pairs (users who passed both)
    passed_df = train_df[train_df['truth'] == 0]
    course_groups = passed_df.groupby('user_id')['course_id'].apply(list)
    pair_counts = Counter()
    for courses in course_groups:
        courses = sorted(list(set(courses)))
        if len(courses) > 1:
            for c1, c2 in itertools.combinations(courses, 2):
                pair_counts[(c1, c2)] += 1

    # 2. Average features of successful users
    success_users = train_df[train_df['truth'] == 0]
    avg_success_features = success_users[ACTION_COLS].mean().to_dict()

    # 3. Popular courses
    popular_courses = train_df['course_id'].value_counts().head(
        50).index.tolist()

    knowledge_base = {
        'pair_counts': pair_counts,
        'popular_courses': popular_courses,
        'avg_features': avg_success_features
    }

    with open(KNOWLEDGE_PATH, 'wb') as f:
        pickle.dump(knowledge_base, f)
    print(
        f"✅ Knowledge Base saved: {len(pair_counts)} course pairs, {len(popular_courses)} popular courses")

    # ===========================
    # [6/6] XUẤT FILE MAPPING ID
    # ===========================
    print("📤 [6/6] Exporting ID Mapping for Producer...")
    valid_users = train_df['user_id'].unique().tolist()
    valid_courses = train_df['course_id'].unique().tolist()

    ids_mapping = {
        'users': valid_users,
        'courses': valid_courses
    }

    with open(IDS_MAPPING_PATH, 'w') as f:
        json.dump(ids_mapping, f)

    print(
        f"✅ IDs Mapping saved: {len(valid_users)} users, {len(valid_courses)} courses")
    print("\n🎉 TRAIN MODULE COMPLETED!")


if __name__ == "__main__":
    main()
