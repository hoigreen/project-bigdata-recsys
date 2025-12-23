# run_consumer.py
# Consumes user events from Kafka and provides real-time recommendations
import json
import time
import pandas as pd
import numpy as np
import lightgbm as lgb
from kafka import KafkaConsumer
from collections import defaultdict, Counter
import os
import sys
import pickle

# ===========================
# CẤU HÌNH
# ===========================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Kafka config - supports both local and container execution
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "mooc-events")

# Artifact directory (same as train_module.py)
ARTIFACT_DIR = os.environ.get("ARTIFACT_DIR", "/tmp/recsys-artifacts")

# Artifact paths
MODEL_PATH = os.path.join(ARTIFACT_DIR, "lgb_model_binary.txt")
HISTORY_PATH = os.path.join(ARTIFACT_DIR, "user_history_snapshot.csv")
LOG_PATH = os.path.join(ARTIFACT_DIR, "new_events_log.csv")
ENCODER_PATH = os.path.join(ARTIFACT_DIR, "course_encoder.pkl")
KNOWLEDGE_PATH = os.path.join(ARTIFACT_DIR, "knowledge_base.pkl")
RECOMMENDATIONS_PATH = os.path.join(
    ARTIFACT_DIR, "realtime_recommendations.json")

# Global state
bst = None
le_course = None
knowledge_base = None

last_model_mtime = 0
last_kb_mtime = 0
last_check_time = 0
CHECK_INTERVAL = 10  # seconds

# ===========================
# HOT RELOAD FUNCTIONS
# ===========================


def load_model():
    global bst, last_model_mtime
    try:
        if os.path.exists(MODEL_PATH):
            mtime = os.path.getmtime(MODEL_PATH)
            if mtime > last_model_mtime:
                print(f"\n♻️  [HOT RELOAD] Updating Model...")
                bst = lgb.Booster(model_file=MODEL_PATH)
                last_model_mtime = mtime
                print("✅ Model Updated!")
    except Exception as e:
        print(f"⚠️ Model Load Error: {e}")


def load_knowledge():
    global knowledge_base, last_kb_mtime
    try:
        if os.path.exists(KNOWLEDGE_PATH):
            mtime = os.path.getmtime(KNOWLEDGE_PATH)
            if mtime > last_kb_mtime:
                print(f"\n♻️  [HOT RELOAD] Updating Knowledge Base...")
                with open(KNOWLEDGE_PATH, 'rb') as f:
                    knowledge_base = pickle.load(f)
                last_kb_mtime = mtime
                print("✅ Knowledge Base Loaded!")
    except Exception as e:
        print(f"⚠️ KB Load Error: {e}")


def load_encoder():
    global le_course
    try:
        if os.path.exists(ENCODER_PATH):
            with open(ENCODER_PATH, 'rb') as f:
                le_course = pickle.load(f)
            print("✅ Course Encoder Loaded.")
        else:
            print("⚠️ Warning: encoder not found.")
    except Exception as e:
        print(f"⚠️ Encoder Load Error: {e}")


# ===========================
# STARTUP
# ===========================
print("\n" + "="*60)
print("   CONSUMER - INTELLIGENT REAL-TIME RECOMMENDATIONS")
print("="*60)
print(f"📡 Kafka: {KAFKA_BOOTSTRAP}")
print(f"📌 Topic: {TOPIC_NAME}")

load_model()
load_knowledge()
load_encoder()

FEATURE_COLS = [
    'course_id_int',
    'action_click_about', 'action_click_courseware', 'action_click_forum', 'action_click_info',
    'action_click_progress', 'action_close_courseware', 'action_close_forum', 'action_create_comment',
    'action_create_thread', 'action_delete_comment', 'action_delete_thread', 'action_load_video',
    'action_pause_video', 'action_play_video', 'action_problem_check', 'action_problem_check_correct',
    'action_problem_check_incorrect', 'action_problem_get', 'action_problem_save', 'action_reset_problem',
    'action_seek_video', 'action_stop_video', 'unique_session_count', 'avg_nActions_per_session'
]

# In-memory state for each user
user_state = defaultdict(
    lambda: {col: 0.0 for col in FEATURE_COLS if col != 'course_id_int'})

# Additional tracking
# Courses passed (don't recommend again)
user_passed_set = defaultdict(set)
user_failed_history = defaultdict(set)  # Courses failed (recommend for retry)
user_truth_history = {}                 # (user, course) -> truth

# Load history snapshot
print("⏳ Loading User History Snapshot...")
if os.path.exists(HISTORY_PATH):
    try:
        df_hist = pd.read_csv(HISTORY_PATH, dtype={'username': str})
        count = 0
        for _, row in df_hist.iterrows():
            u = str(row['username'])
            c = row['course_id']
            key = (u, c)

            # Restore behavior to RAM
            action_data = {k: v for k, v in row.items()
                           if k in user_state.default_factory()}
            user_state[key] = action_data

            # Classify Pass/Fail
            if pd.notna(row['truth']):
                status = int(row['truth'])
                user_truth_history[key] = status
                if status == 0:
                    user_passed_set[u].add(c)
                elif status == 1:
                    user_failed_history[u].add(c)
            count += 1
        print(f"✅ Loaded history for {count} records.")
    except Exception as e:
        print(f"⚠️ History Load Error: {e}")
else:
    print("⚠️ History snapshot not found. Starting fresh.")

# Connect to Kafka
print(f"🔌 Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000  # Timeout to allow checking for hot reload
    )
    print("🚀 READY! Waiting for events...")
except Exception as e:
    print(f"❌ Kafka Error: {e}")
    print("   Make sure Kafka is running.")
    sys.exit(1)

# ===========================
# MAIN LOOP
# ===========================
event_count = 0
while True:
    try:
        # Hot Reload check
        if time.time() - last_check_time > CHECK_INTERVAL:
            load_model()
            load_knowledge()
            last_check_time = time.time()

        # Poll for messages
        for message in consumer:
            event_count += 1

            # Parse event
            event = message.value
            user = str(event['username'])
            course = event['course_id']
            action = event['action']

            # Log to file
            with open(LOG_PATH, "a") as f:
                f.write(f"{user},{course},{action},{time.time()}\n")

            short_user = str(user)[:10] + \
                "..." if len(str(user)) > 10 else str(user)

            # Update state in RAM
            current_features = user_state[(user, course)]
            if action in current_features:
                current_features[action] += 1

            # Recalculate derived features
            action_only_cols = [
                c for c in FEATURE_COLS if c.startswith('action_')]
            total_acts = sum(current_features.get(k, 0)
                             for k in action_only_cols)
            current_features['avg_nActions_per_session'] = total_acts

            # Only process recommendations if model is loaded
            if bst and le_course and knowledge_base:

                # --- A: CURRENT COURSE PREDICTION ---
                try:
                    c_int = le_course.transform([course])[0]
                except:
                    c_int = 0

                # Build input vector from actual behavior
                input_vec = []
                for col in FEATURE_COLS:
                    if col == 'course_id_int':
                        input_vec.append(c_int)
                    else:
                        input_vec.append(current_features.get(col, 0))

                fail_prob = bst.predict(pd.DataFrame(
                    [input_vec], columns=FEATURE_COLS))[0]
                pass_prob = (1 - fail_prob) * 100

                # --- B: SMART RECOMMENDATIONS ---
                candidates = set()
                pair_counts = knowledge_base['pair_counts']
                popular = knowledge_base['popular_courses']
                avg_stats = knowledge_base['avg_features']

                # B1. Collect candidates
                # 1. Previously failed courses (for retry)
                for failed_c in user_failed_history[user]:
                    if failed_c != course:
                        candidates.add(failed_c)

                # 2. Related courses (Collaborative Filtering)
                for (c1, c2), count in pair_counts.most_common(50):
                    if c1 == course and c2 not in user_passed_set[user] and c2 != course:
                        candidates.add(c2)
                    elif c2 == course and c1 not in user_passed_set[user] and c1 != course:
                        candidates.add(c1)

                # 3. Popular courses (fallback)
                if len(candidates) < 5:
                    for p in popular:
                        if p not in user_passed_set[user] and p != course:
                            candidates.add(p)
                        if len(candidates) >= 10:
                            break

                # B2. Score candidates
                scored_candidates = []
                for cand in list(candidates)[:15]:
                    try:
                        cand_int = le_course.transform([cand])[0]
                        cand_vec = []

                        # Check if retake
                        is_retake = False
                        if (user, cand) in user_state and user_state[(user, cand)].get('avg_nActions_per_session', 0) > 0:
                            is_retake = True
                            user_history_features = user_state[(user, cand)]

                            # Weighted blending: 30% history + 70% avg success profile
                            w_history = 0.3
                            w_avg = 0.7

                            for col in FEATURE_COLS:
                                if col == 'course_id_int':
                                    cand_vec.append(cand_int)
                                else:
                                    hist_val = user_history_features.get(
                                        col, 0)
                                    avg_val = avg_stats.get(col, 0)
                                    blended_val = (
                                        hist_val * w_history) + (avg_val * w_avg)
                                    cand_vec.append(blended_val)
                        else:
                            # New course -> use 100% average success profile
                            for col in FEATURE_COLS:
                                if col == 'course_id_int':
                                    cand_vec.append(cand_int)
                                else:
                                    cand_vec.append(avg_stats.get(col, 0))

                        p_fail = bst.predict(pd.DataFrame(
                            [cand_vec], columns=FEATURE_COLS))[0]
                        scored_candidates.append((cand, p_fail, is_retake))
                    except:
                        continue

                # Sort by lowest fail probability (highest pass chance)
                scored_candidates.sort(key=lambda x: x[1])

                # --- OUTPUT ---
                print("\n" + "-"*60)
                print(f"👤 USER: {short_user} | Event #{event_count}")
                print(f"📚 CURRENT: {course}")

                # Status display
                past_status = user_truth_history.get((user, course), -1)
                if past_status == 1:
                    status_str = "Failed Previously (Retaking)"
                elif past_status == 0:
                    status_str = "Passed Previously (Reviewing)"
                else:
                    if total_acts <= 1:
                        status_str = "New Learner"
                    else:
                        status_str = "Currently Studying"

                print(f"📜 Status: {status_str}")
                print(f"📊 Current Pass Prob: {pass_prob:.1f}%")

                print(f"\n💡 SMART RECOMMENDATIONS:")

                # Build recommendations list for dashboard
                recommendations_list = []
                if scored_candidates:
                    for i, (rec_course, score, is_retake) in enumerate(scored_candidates[:5], 1):
                        chance = (1 - score) * 100
                        note = " (Retake ⚠️)" if is_retake else " (New 🆕)"
                        short_rec = rec_course[:25] + \
                            "..." if len(rec_course) > 25 else rec_course
                        print(
                            f"   {i}. {short_rec:<28} | Chance: {chance:.1f}% {note}")

                        # Add to recommendations list
                        recommendations_list.append({
                            "rank": i,
                            "course_id": rec_course,
                            "pass_chance": round(chance, 1),
                            "is_retake": is_retake
                        })
                else:
                    print("   (Gathering data...)")

                # Save recommendations to JSON for dashboard
                try:
                    rec_data = {
                        "user_id": user,
                        "current_course": course,
                        "status": status_str,
                        "current_pass_prob": round(pass_prob, 1),
                        "recommendations": recommendations_list,
                        "timestamp": time.time(),
                        "event_count": event_count
                    }
                    with open(RECOMMENDATIONS_PATH, 'w') as f:
                        json.dump(rec_data, f, indent=2)
                except Exception as e:
                    print(f"⚠️ Failed to save recommendations: {e}")

                print("-" * 60 + "\n")
            else:
                print(
                    f"📥 [{event_count}] Recv: {short_user} | {action} (Model not ready)", end='\r')

    except KeyboardInterrupt:
        print(f"\n🛑 Consumer stopped. Processed {event_count} events.")
        break
    except Exception as e:
        # Log error but continue
        print(f"⚠️ Error: {e}")
        time.sleep(1)
