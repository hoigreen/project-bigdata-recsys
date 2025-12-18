CREATE TABLE IF NOT EXISTS users (
    user_id      BIGINT PRIMARY KEY,
    gender       VARCHAR(10),
    education    VARCHAR(50),
    birth_year   INT,
    created_at   TIMESTAMP DEFAULT NOW()
);

-- Bảng interactions: train/test.csv sau khi chuẩn hóa đúng schema LGBM
CREATE TABLE IF NOT EXISTS interactions (
    user_id    BIGINT,
    course_id  TEXT,
    truth      SMALLINT,
    action_click_about               DOUBLE PRECISION,
    action_click_courseware          DOUBLE PRECISION,
    action_click_forum               DOUBLE PRECISION,
    action_click_info                DOUBLE PRECISION,
    action_click_progress            DOUBLE PRECISION,
    action_close_courseware          DOUBLE PRECISION,
    action_close_forum               DOUBLE PRECISION,
    action_create_comment            DOUBLE PRECISION,
    action_create_thread             DOUBLE PRECISION,
    action_delete_comment            DOUBLE PRECISION,
    action_delete_thread             DOUBLE PRECISION,
    action_load_video                DOUBLE PRECISION,
    action_pause_video               DOUBLE PRECISION,
    action_play_video                DOUBLE PRECISION,
    action_problem_check             DOUBLE PRECISION,
    action_problem_check_correct     DOUBLE PRECISION,
    action_problem_check_incorrect   DOUBLE PRECISION,
    action_problem_get               DOUBLE PRECISION,
    action_problem_save              DOUBLE PRECISION,
    action_reset_problem             DOUBLE PRECISION,
    action_seek_video                DOUBLE PRECISION,
    action_stop_video                DOUBLE PRECISION,
    unique_session_count             DOUBLE PRECISION,
    avg_nActions_per_session         DOUBLE PRECISION,
    event_ts   BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, course_id)
);

-- ALS latent factors tables (created by Spark ALS job)
CREATE TABLE IF NOT EXISTS als_user_factors (
    user_id BIGINT PRIMARY KEY,
    factors TEXT  -- JSON array of latent factors
);

CREATE TABLE IF NOT EXISTS als_item_factors (
    course_id TEXT PRIMARY KEY,
    factors TEXT  -- JSON array of latent factors
);
