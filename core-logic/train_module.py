# train_module.py
# Train LightGBM model from Postgres data (loaded via ETL from MinIO)
# Supports periodic retraining with model versioning and metrics logging
import pandas as pd
import numpy as np
import lightgbm as lgb
import os
import sys
import time
import shutil
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, log_loss
from sklearn.preprocessing import LabelEncoder
from collections import Counter
import itertools
import pickle
import json
import psycopg2
from psycopg2.extras import execute_values

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

# Model versioning settings
ENABLE_VERSIONING = os.environ.get(
    "ENABLE_MODEL_VERSIONING", "true").lower() == "true"
KEEP_N_VERSIONS = int(os.environ.get("KEEP_MODEL_VERSIONS", 5))

# Generate model version (timestamp-based)
MODEL_VERSION = datetime.now().strftime("%Y%m%d_%H%M%S")

MODEL_PATH = os.path.join(ARTIFACT_DIR, "lgb_model_binary.txt")
HISTORY_PATH = os.path.join(ARTIFACT_DIR, "user_history_snapshot.csv")
ENCODER_PATH = os.path.join(ARTIFACT_DIR, "course_encoder.pkl")
KNOWLEDGE_PATH = os.path.join(ARTIFACT_DIR, "knowledge_base.pkl")
IDS_MAPPING_PATH = os.path.join(ARTIFACT_DIR, "ids_mapping.json")

# Versioned paths (for model history)
VERSIONS_DIR = os.path.join(ARTIFACT_DIR, "versions")
os.makedirs(VERSIONS_DIR, exist_ok=True)


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )


def ensure_training_history_table():
    """Create model_training_history table if it doesn't exist."""
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS model_training_history (
                id SERIAL PRIMARY KEY,
                model_name VARCHAR(50) NOT NULL,
                model_version VARCHAR(100) NOT NULL,
                training_samples INT,
                validation_samples INT,
                train_auc DOUBLE PRECISION,
                valid_auc DOUBLE PRECISION,
                train_logloss DOUBLE PRECISION,
                valid_logloss DOUBLE PRECISION,
                train_accuracy DOUBLE PRECISION,
                valid_accuracy DOUBLE PRECISION,
                num_features INT,
                num_courses INT,
                num_users INT,
                hyperparameters TEXT,
                artifact_path TEXT,
                training_duration_seconds DOUBLE PRECISION,
                data_snapshot_timestamp TIMESTAMP,
                is_active BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                notes TEXT
            )
        """)
        conn.commit()
    except Exception as e:
        print(f"⚠️  Warning: Could not create training history table: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def log_training_metrics(metrics: dict):
    """Log training metrics to PostgreSQL for tracking model versions."""
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        # First, deactivate any previously active model of same type
        cur.execute("""
            UPDATE model_training_history 
            SET is_active = FALSE 
            WHERE model_name = %s AND is_active = TRUE
        """, (metrics['model_name'],))

        # Insert new training record as active
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
            metrics['train_auc'],
            metrics['valid_auc'],
            metrics['train_logloss'],
            metrics['valid_logloss'],
            metrics['train_accuracy'],
            metrics['valid_accuracy'],
            metrics['num_features'],
            metrics['num_courses'],
            metrics['num_users'],
            json.dumps(metrics['hyperparameters']),
            metrics['artifact_path'],
            metrics['training_duration_seconds'],
            metrics['data_snapshot_timestamp'],
            metrics.get('notes', f'Periodic retraining at {datetime.now()}')
        ))
        conn.commit()
        print(
            f"✅ Training metrics logged to database (version: {metrics['model_version']})")
    except Exception as e:
        print(f"⚠️  Warning: Could not log training metrics: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def save_versioned_artifacts(version: str):
    """Save a copy of current artifacts to versioned directory."""
    if not ENABLE_VERSIONING:
        return

    version_dir = os.path.join(VERSIONS_DIR, version)
    os.makedirs(version_dir, exist_ok=True)

    artifacts = [
        (MODEL_PATH, "lgb_model_binary.txt"),
        (ENCODER_PATH, "course_encoder.pkl"),
        (KNOWLEDGE_PATH, "knowledge_base.pkl"),
        (HISTORY_PATH, "user_history_snapshot.csv"),
        (IDS_MAPPING_PATH, "ids_mapping.json")
    ]

    for src_path, filename in artifacts:
        if os.path.exists(src_path):
            dst_path = os.path.join(version_dir, filename)
            shutil.copy2(src_path, dst_path)

    print(f"✅ Versioned artifacts saved to: {version_dir}")

    # Cleanup old versions (keep only KEEP_N_VERSIONS most recent)
    cleanup_old_versions()


def cleanup_old_versions():
    """Remove old model versions, keeping only the most recent N versions."""
    if not ENABLE_VERSIONING:
        return

    try:
        versions = sorted([
            d for d in os.listdir(VERSIONS_DIR)
            if os.path.isdir(os.path.join(VERSIONS_DIR, d))
        ], reverse=True)  # Most recent first

        if len(versions) > KEEP_N_VERSIONS:
            for old_version in versions[KEEP_N_VERSIONS:]:
                old_path = os.path.join(VERSIONS_DIR, old_version)
                shutil.rmtree(old_path)
                print(f"🗑️  Removed old model version: {old_version}")
    except Exception as e:
        print(f"⚠️  Warning: Could not cleanup old versions: {e}")


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
    # Start timing the training process
    training_start_time = time.time()
    data_snapshot_timestamp = datetime.now()

    print("="*60)
    print(f"   LGBM MODEL TRAINING - Version: {MODEL_VERSION}")
    print("="*60)

    # Ensure training history table exists
    ensure_training_history_table()

    print("\n⏳ [1/7] Reading data from Postgres...")

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

    # Capture data statistics
    num_users = train_df['user_id'].nunique()
    num_courses = train_df['course_id'].nunique()

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
    # [2/7] HUẤN LUYỆN MODEL
    # ===========================
    print("💪 [2/7] Training LightGBM Model...")
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y)

    lgb_train = lgb.Dataset(X_train, label=y_train,
                            categorical_feature=['course_id_int'])
    lgb_val = lgb.Dataset(X_val, label=y_val, reference=lgb_train)

    # Hyperparameters (configurable via environment variables)
    params = {
        'objective': 'binary',
        'metric': ['auc', 'binary_logloss'],
        'learning_rate': float(os.environ.get('LGBM_LEARNING_RATE', 0.05)),
        'num_leaves': int(os.environ.get('LGBM_NUM_LEAVES', 31)),
        'seed': 42,
        'verbose': -1
    }
    num_boost_round = int(os.environ.get('LGBM_NUM_BOOST_ROUND', 500))
    early_stopping_rounds = int(os.environ.get('LGBM_EARLY_STOPPING', 50))

    clf = lgb.train(
        params, lgb_train, num_boost_round=num_boost_round,
        valid_sets=[lgb_train, lgb_val],
        callbacks=[lgb.early_stopping(
            early_stopping_rounds), lgb.log_evaluation(0)]
    )
    clf.save_model(MODEL_PATH)
    print(f"✅ Model saved at: {MODEL_PATH}")

    # ===========================
    # [3/7] ĐÁNH GIÁ
    # ===========================
    print("\n" + "="*60)
    print("   [3/7] PERFORMANCE REPORT")
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

    train_metrics = calculate_metrics('TRAIN', X_train, y_train, clf)
    valid_metrics = calculate_metrics('VALID', X_val, y_val, clf)

    metrics_list = [train_metrics, valid_metrics]

    results_df = pd.DataFrame(
        [m for m in metrics_list if m is not None]).set_index('Dataset')
    print(results_df)
    print("="*60 + "\n")

    # ===========================
    # [4/7] TẠO SNAPSHOT LỊCH SỬ
    # ===========================
    print("💾 [4/7] Exporting history snapshot...")
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
    # [5/7] TẠO KNOWLEDGE BASE
    # ===========================
    print("\n🔍 [5/7] Building Knowledge Base...")

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
    # [6/7] XUẤT FILE MAPPING ID
    # ===========================
    print("📤 [6/7] Exporting ID Mapping for Producer...")
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

    # ===========================
    # [7/7] LOG METRICS & VERSION ARTIFACTS
    # ===========================
    training_duration = time.time() - training_start_time

    print(f"\n📊 [7/7] Logging training metrics and saving version...")

    # Prepare metrics for database logging
    full_metrics = {
        'model_name': 'lgbm_dropout',
        'model_version': MODEL_VERSION,
        'training_samples': len(X_train),
        'validation_samples': len(X_val),
        'train_auc': train_metrics['AUC'] if train_metrics else None,
        'valid_auc': valid_metrics['AUC'] if valid_metrics else None,
        'train_logloss': train_metrics['Logloss'] if train_metrics else None,
        'valid_logloss': valid_metrics['Logloss'] if valid_metrics else None,
        'train_accuracy': train_metrics['Accuracy (%)'] if train_metrics else None,
        'valid_accuracy': valid_metrics['Accuracy (%)'] if valid_metrics else None,
        'num_features': len(FEATURE_COLS),
        'num_courses': num_courses,
        'num_users': num_users,
        'hyperparameters': {
            **params,
            'num_boost_round': num_boost_round,
            'early_stopping_rounds': early_stopping_rounds,
            'actual_iterations': clf.best_iteration
        },
        'artifact_path': ARTIFACT_DIR,
        'training_duration_seconds': training_duration,
        'data_snapshot_timestamp': data_snapshot_timestamp,
        'notes': f'Periodic retraining - {len(train_df)} total samples'
    }

    # Log to database
    log_training_metrics(full_metrics)

    # Save versioned artifacts
    if ENABLE_VERSIONING:
        save_versioned_artifacts(MODEL_VERSION)

    print("\n" + "="*60)
    print("   TRAINING SUMMARY")
    print("="*60)
    print(f"   Model Version:     {MODEL_VERSION}")
    print(f"   Training Duration: {training_duration:.2f} seconds")
    print(f"   Training Samples:  {len(X_train)}")
    print(
        f"   Validation AUC:    {valid_metrics['AUC']:.4f}" if valid_metrics else "")
    print(f"   Total Users:       {num_users}")
    print(f"   Total Courses:     {num_courses}")
    print("="*60)

    print("\n🎉 TRAIN MODULE COMPLETED!")


if __name__ == "__main__":
    main()
