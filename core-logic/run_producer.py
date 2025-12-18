# run_producer.py
# Simulates user events and sends them to Kafka
import time
import json
import random
import os
import sys
from kafka import KafkaProducer

# ===========================
# CẤU HÌNH
# ===========================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Artifact directory (same as train_module.py)
ARTIFACT_DIR = os.environ.get("ARTIFACT_DIR", "/tmp/recsys-artifacts")
IDS_MAPPING_PATH = os.path.join(ARTIFACT_DIR, "ids_mapping.json")

# Kafka config - supports both local and container execution
# Local: localhost:9092, Container: kafka:29092
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "mooc-events")

print("\n" + "="*60)
print("   PRODUCER - USER BEHAVIOR SIMULATOR")
print("="*60)
print(f"📡 Kafka: {KAFKA_BOOTSTRAP}")
print(f"📌 Topic: {TOPIC_NAME}")

# 1. Load IDs from JSON file
REAL_USERS = []
REAL_COURSES = []

if os.path.exists(IDS_MAPPING_PATH):
    print(f"⏳ Loading valid IDs from: {IDS_MAPPING_PATH}")
    try:
        with open(IDS_MAPPING_PATH, 'r') as f:
            data = json.load(f)
            REAL_USERS = data.get('users', [])
            REAL_COURSES = data.get('courses', [])

        # Select random subset for demo
        num_users = min(10, len(REAL_USERS))
        num_courses = min(10, len(REAL_COURSES))

        if len(REAL_USERS) > num_users:
            DEMO_USERS = random.sample(REAL_USERS, num_users)
        else:
            DEMO_USERS = REAL_USERS

        if len(REAL_COURSES) > num_courses:
            DEMO_COURSES = random.sample(REAL_COURSES, num_courses)
        else:
            DEMO_COURSES = REAL_COURSES

        print(
            f"✅ Loaded {len(REAL_USERS)} Users and {len(REAL_COURSES)} Courses.")
        print(f"   -> Simulating traffic for {len(DEMO_USERS)} active users.")
    except Exception as e:
        print(f"❌ JSON Read Error: {e}")
        sys.exit(1)
else:
    print("❌ ERROR: ids_mapping.json not found.")
    print("   -> Run 'train_module.py' first!")
    sys.exit(1)

# 2. Available actions
ACTIONS = [
    'action_play_video', 'action_pause_video', 'action_seek_video',
    'action_stop_video', 'action_problem_check', 'action_problem_check_correct',
    'action_problem_check_incorrect', 'action_click_forum', 'action_create_comment',
    'action_click_courseware', 'action_close_courseware', 'action_load_video',
    'action_click_about', 'action_click_progress'
]

# 3. Connect to Kafka
print(f"🔌 Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        retries=5,
        retry_backoff_ms=1000
    )
    print("🚀 READY! Sending events (Ctrl+C to stop)...")
except Exception as e:
    print(f"❌ Kafka Connection Error: {e}")
    print("   Make sure Kafka is running.")
    sys.exit(1)

# 4. Event loop
event_count = 0
try:
    while True:
        user = random.choice(DEMO_USERS)
        course = random.choice(DEMO_COURSES)
        action = random.choice(ACTIONS)

        event = {
            'username': user,
            'course_id': course,
            'action': action,
            'timestamp': time.time()
        }

        producer.send(TOPIC_NAME, value=event)
        event_count += 1

        # Short log
        short_user = str(user)[:10] + \
            "..." if len(str(user)) > 10 else str(user)
        short_course = str(course)[:20] + \
            "..." if len(str(course)) > 20 else str(course)

        print(f"📤 [{event_count}] Sent: {short_user} | {short_course} | {action}")

        # Random delay between events
        time.sleep(random.uniform(1.0, 3.0))

except KeyboardInterrupt:
    print(f"\n🛑 Producer stopped. Sent {event_count} events.")
    producer.close()
