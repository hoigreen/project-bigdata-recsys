"""
Script to add synthetic interaction data to PostgreSQL
This allows demonstrating model improvement with more diverse data
"""

import psycopg2
import random
import os
from datetime import datetime, timedelta

# PostgreSQL config
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", 5433))
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "postgres")
PG_DB = os.environ.get("PG_DB", "recsys")


def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )


def add_synthetic_interactions(n_samples=5000):
    """Add synthetic interaction data with varied behavior patterns"""

    conn = get_connection()
    cur = conn.cursor()

    # Get existing users and courses from interactions table
    cur.execute("SELECT DISTINCT user_id FROM interactions LIMIT 1000")
    users = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT DISTINCT course_id FROM interactions LIMIT 100")
    courses = [row[0] for row in cur.fetchall()]

    if not users or not courses:
        print("❌ No users or courses found. Run ETL first.")
        return

    print(f"📊 Found {len(users)} users and {len(courses)} courses")
    print(f"🔧 Generating {n_samples} synthetic interactions...")

    # Action columns
    action_cols = [
        'action_click_about', 'action_click_courseware', 'action_click_forum',
        'action_click_info', 'action_click_progress', 'action_close_courseware',
        'action_close_forum', 'action_create_comment', 'action_create_thread',
        'action_delete_comment', 'action_delete_thread', 'action_load_video',
        'action_pause_video', 'action_play_video', 'action_problem_check',
        'action_problem_check_correct', 'action_problem_check_incorrect',
        'action_problem_get', 'action_problem_save', 'action_reset_problem',
        'action_seek_video', 'action_stop_video'
    ]

    inserted = 0

    for i in range(n_samples):
        user_id = random.choice(users)
        course_id = random.choice(courses)

        # Create varied behavior patterns
        engagement_level = random.choice(['low', 'medium', 'high'])

        # Generate actions based on engagement
        actions = {}
        for col in action_cols:
            if engagement_level == 'high':
                actions[col] = random.randint(5, 50)
            elif engagement_level == 'medium':
                actions[col] = random.randint(1, 20)
            else:
                actions[col] = random.randint(0, 5)

        # Sessions and activity
        unique_sessions = random.randint(
            1, 30) if engagement_level == 'high' else random.randint(1, 10)
        total_actions = sum(actions.values())
        avg_actions = total_actions / unique_sessions if unique_sessions > 0 else 0

        # Truth label (higher engagement = more likely to pass)
        if engagement_level == 'high':
            truth = 0 if random.random() < 0.85 else 1  # 85% pass rate
        elif engagement_level == 'medium':
            truth = 0 if random.random() < 0.60 else 1  # 60% pass rate
        else:
            truth = 0 if random.random() < 0.30 else 1  # 30% pass rate

        # Insert to database
        import time as time_module
        event_ts = int(time_module.time() * 1000)  # milliseconds

        try:
            cur.execute("""
                INSERT INTO interactions (
                    user_id, course_id, truth,
                    action_click_about, action_click_courseware, action_click_forum,
                    action_click_info, action_click_progress, action_close_courseware,
                    action_close_forum, action_create_comment, action_create_thread,
                    action_delete_comment, action_delete_thread, action_load_video,
                    action_pause_video, action_play_video, action_problem_check,
                    action_problem_check_correct, action_problem_check_incorrect,
                    action_problem_get, action_problem_save, action_reset_problem,
                    action_seek_video, action_stop_video,
                    unique_session_count, avg_nactions_per_session,
                    event_ts, created_at
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, NOW()
                )
            """, (
                user_id, course_id, truth,
                actions['action_click_about'], actions['action_click_courseware'],
                actions['action_click_forum'], actions['action_click_info'],
                actions['action_click_progress'], actions['action_close_courseware'],
                actions['action_close_forum'], actions['action_create_comment'],
                actions['action_create_thread'], actions['action_delete_comment'],
                actions['action_delete_thread'], actions['action_load_video'],
                actions['action_pause_video'], actions['action_play_video'],
                actions['action_problem_check'], actions['action_problem_check_correct'],
                actions['action_problem_check_incorrect'], actions['action_problem_get'],
                actions['action_problem_save'], actions['action_reset_problem'],
                actions['action_seek_video'], actions['action_stop_video'],
                unique_sessions, avg_actions, event_ts
            ))
            inserted += 1
        except Exception as e:
            # Skip duplicates or errors
            pass

        if (i + 1) % 1000 == 0:
            print(f"   Progress: {i + 1}/{n_samples}")

    conn.commit()

    # Verify count
    cur.execute("SELECT COUNT(*) FROM interactions")
    total = cur.fetchone()[0]

    print(f"\n✅ Added {inserted} new interactions")
    print(f"📊 Total interactions in database: {total}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    add_synthetic_interactions(n)
