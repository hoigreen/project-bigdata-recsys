"""
Script to modify existing interaction data to simulate evolving user behavior
This creates variation in training data to demonstrate model improvement
"""

import psycopg2
import random
import os

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


def modify_interactions(n_samples=5000, noise_factor=0.2):
    """
    Modify a subset of interactions to add variation
    This simulates users changing their behavior over time
    """

    conn = get_connection()
    cur = conn.cursor()

    # Get current statistics
    cur.execute("SELECT COUNT(*) FROM interactions")
    total = cur.fetchone()[0]
    print(f"📊 Total interactions: {total}")

    # Get sample of interactions to modify
    cur.execute(f"""
        SELECT user_id, course_id, truth,
               action_click_about, action_click_courseware, action_click_forum,
               action_load_video, action_play_video, action_problem_check,
               action_problem_check_correct, action_problem_check_incorrect
        FROM interactions
        ORDER BY RANDOM()
        LIMIT {n_samples}
    """)

    rows = cur.fetchall()
    print(
        f"🔧 Modifying {len(rows)} interactions with noise factor {noise_factor}...")

    modified = 0
    for row in rows:
        user_id, course_id, old_truth = row[0], row[1], row[2]

        # Add noise to action counts
        noise_multiplier = 1 + random.uniform(-noise_factor, noise_factor)

        # Sometimes flip the truth label (simulate re-evaluation)
        new_truth = old_truth
        if random.random() < 0.05:  # 5% chance to flip
            new_truth = 1 - old_truth if old_truth in [0, 1] else old_truth

        # Modify action counts with some noise
        action_changes = {}
        action_cols = [
            'action_click_about', 'action_click_courseware', 'action_click_forum',
            'action_load_video', 'action_play_video', 'action_problem_check',
            'action_problem_check_correct', 'action_problem_check_incorrect'
        ]

        for i, col in enumerate(action_cols):
            old_val = row[3 + i] or 0
            new_val = max(0, old_val * noise_multiplier +
                          random.randint(-2, 5))
            action_changes[col] = new_val

        # Update record
        try:
            cur.execute("""
                UPDATE interactions
                SET truth = %s,
                    action_click_about = %s,
                    action_click_courseware = %s,
                    action_click_forum = %s,
                    action_load_video = %s,
                    action_play_video = %s,
                    action_problem_check = %s,
                    action_problem_check_correct = %s,
                    action_problem_check_incorrect = %s,
                    created_at = NOW()
                WHERE user_id = %s AND course_id = %s
            """, (
                new_truth,
                action_changes['action_click_about'],
                action_changes['action_click_courseware'],
                action_changes['action_click_forum'],
                action_changes['action_load_video'],
                action_changes['action_play_video'],
                action_changes['action_problem_check'],
                action_changes['action_problem_check_correct'],
                action_changes['action_problem_check_incorrect'],
                user_id, course_id
            ))
            modified += 1
        except Exception as e:
            print(f"⚠️ Error: {e}")

    conn.commit()

    print(f"\n✅ Modified {modified} interactions")
    print("📊 New data distribution will affect next training run")

    # Show sample of changes
    cur.execute("""
        SELECT truth, COUNT(*) 
        FROM interactions 
        GROUP BY truth 
        ORDER BY truth
    """)
    print("\n📈 Truth label distribution:")
    for row in cur.fetchall():
        label = "Pass" if row[0] == 0 else (
            "Fail" if row[0] == 1 else "Unknown")
        print(f"   {label}: {row[1]:,}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    import sys
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    noise = float(sys.argv[2]) if len(sys.argv) > 2 else 0.3
    modify_interactions(n, noise)
