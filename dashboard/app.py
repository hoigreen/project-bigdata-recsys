"""
RecSys Monitoring Dashboard
Real-time monitoring and analytics for the recommendation system
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from psycopg2 import OperationalError
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
import os
import json
import time
from datetime import datetime, timedelta
from collections import deque
import requests
import threading
from streamlit_autorefresh import st_autorefresh

# ===========================
# PAGE CONFIG
# ===========================
st.set_page_config(
    page_title="RecSys Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===========================
# CONFIGURATION
# ===========================
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", 5433))
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "postgres")
PG_DB = os.environ.get("PG_DB", "recsys")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "mooc-events")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
SPARK_MASTER_UI = os.environ.get("SPARK_MASTER_UI", "http://localhost:8081")
AIRFLOW_UI = os.environ.get("AIRFLOW_UI", "http://localhost:8080")

ARTIFACT_DIR = os.environ.get("ARTIFACT_DIR", "/tmp/recsys-artifacts")
LOG_PATH = os.path.join(ARTIFACT_DIR, "new_events_log.csv")
RECOMMENDATIONS_PATH = os.path.join(
    ARTIFACT_DIR, "realtime_recommendations.json")

# ===========================
# STYLING
# ===========================
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        padding: 1rem;
        color: white;
    }
    .status-healthy {
        color: #00c853;
        font-weight: bold;
    }
    .status-unhealthy {
        color: #ff5252;
        font-weight: bold;
    }
    .status-warning {
        color: #ffc107;
        font-weight: bold;
    }
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
        background-color: #f0f2f6;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# ===========================
# DATABASE CONNECTION
# ===========================


@st.cache_resource(ttl=30)
def get_pg_connection():
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DB
        )
        return conn
    except OperationalError as e:
        return None


def check_postgres_health():
    """Check PostgreSQL connection status"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname=PG_DB, connect_timeout=3
        )
        conn.close()
        return True, "Connected"
    except Exception as e:
        return False, str(e)[:50]


def check_kafka_health():
    """Check Kafka connection status"""
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            request_timeout_ms=3000
        )
        topics = admin.list_topics()
        admin.close()
        return True, f"{len(topics)} topics"
    except Exception as e:
        return False, str(e)[:50]


def check_minio_health():
    """Check MinIO connection status"""
    try:
        response = requests.get(
            f"http://{MINIO_ENDPOINT}/minio/health/live",
            timeout=3
        )
        if response.status_code == 200:
            return True, "Healthy"
        return False, f"Status: {response.status_code}"
    except Exception as e:
        return False, str(e)[:50]


def check_spark_health():
    """Check Spark Master status"""
    try:
        response = requests.get(f"{SPARK_MASTER_UI}/json", timeout=3)
        if response.status_code == 200:
            data = response.json()
            workers = len(data.get('workers', []))
            return True, f"{workers} workers"
        return False, f"Status: {response.status_code}"
    except Exception as e:
        return False, str(e)[:50]


def check_airflow_health():
    """Check Airflow status"""
    try:
        response = requests.get(f"{AIRFLOW_UI}/health", timeout=3)
        if response.status_code == 200:
            return True, "Healthy"
        return False, f"Status: {response.status_code}"
    except Exception as e:
        return False, str(e)[:50]

# ===========================
# DATA FETCHING FUNCTIONS
# ===========================


@st.cache_data(ttl=10)
def get_database_stats():
    """Get statistics from PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname=PG_DB
        )

        stats = {}

        # Users count
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users")
            stats['total_users'] = cur.fetchone()[0]

        # Interactions count
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM interactions")
            stats['total_interactions'] = cur.fetchone()[0]

        # ALS User Factors count
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM als_user_factors")
            stats['als_user_factors'] = cur.fetchone()[0]

        # ALS Item Factors count
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM als_item_factors")
            stats['als_item_factors'] = cur.fetchone()[0]

        # Pass/Fail distribution
        with conn.cursor() as cur:
            cur.execute("""
                SELECT truth, COUNT(*) 
                FROM interactions 
                WHERE truth IS NOT NULL 
                GROUP BY truth
            """)
            result = cur.fetchall()
            stats['pass_count'] = 0
            stats['fail_count'] = 0
            for row in result:
                if row[0] == 0:
                    stats['pass_count'] = row[1]
                elif row[0] == 1:
                    stats['fail_count'] = row[1]

        # Unique courses
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT course_id) FROM interactions")
            stats['unique_courses'] = cur.fetchone()[0]

        conn.close()
        return stats
    except Exception as e:
        return {
            'total_users': 0,
            'total_interactions': 0,
            'als_user_factors': 0,
            'als_item_factors': 0,
            'pass_count': 0,
            'fail_count': 0,
            'unique_courses': 0,
            'error': str(e)
        }


@st.cache_data(ttl=10)
def get_interactions_data():
    """Get interactions data for analysis"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname=PG_DB
        )

        query = """
            SELECT 
                user_id, course_id, truth,
                action_play_video, action_pause_video, action_problem_check,
                action_problem_check_correct, action_problem_check_incorrect,
                action_click_forum, action_create_comment,
                created_at
            FROM interactions
            ORDER BY created_at DESC
            LIMIT 1000
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=10)
def get_course_stats():
    """Get course statistics"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname=PG_DB
        )

        query = """
            SELECT 
                course_id,
                COUNT(*) as total_enrollments,
                SUM(CASE WHEN truth = 0 THEN 1 ELSE 0 END) as pass_count,
                SUM(CASE WHEN truth = 1 THEN 1 ELSE 0 END) as fail_count,
                AVG(action_play_video) as avg_video_plays,
                AVG(action_problem_check) as avg_problem_checks
            FROM interactions
            WHERE truth IS NOT NULL
            GROUP BY course_id
            ORDER BY total_enrollments DESC
            LIMIT 50
        """
        df = pd.read_sql(query, conn)
        conn.close()

        if not df.empty:
            df['pass_rate'] = df['pass_count'] / \
                (df['pass_count'] + df['fail_count']) * 100

        return df
    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=10)
def get_user_stats():
    """Get user statistics"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname=PG_DB
        )

        query = """
            SELECT 
                u.user_id,
                u.gender,
                u.education,
                u.birth_year,
                COUNT(i.course_id) as course_count,
                SUM(CASE WHEN i.truth = 0 THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN i.truth = 1 THEN 1 ELSE 0 END) as failed
            FROM users u
            LEFT JOIN interactions i ON u.user_id = i.user_id
            GROUP BY u.user_id, u.gender, u.education, u.birth_year
            ORDER BY course_count DESC
            LIMIT 100
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()


def get_recent_events():
    """Get recent events from log file"""
    events = []
    try:
        if os.path.exists(LOG_PATH):
            with open(LOG_PATH, 'r') as f:
                lines = f.readlines()[-100:]  # Last 100 events
                for line in lines:
                    parts = line.strip().split(',')
                    if len(parts) >= 4:
                        events.append({
                            'user_id': parts[0],
                            'course_id': parts[1],
                            'action': parts[2],
                            'timestamp': datetime.fromtimestamp(float(parts[3]))
                        })
    except Exception as e:
        pass
    return pd.DataFrame(events)


def get_kafka_topic_stats():
    """Get Kafka topic statistics"""
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            consumer_timeout_ms=1000
        )

        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if partitions:
            total_messages = 0
            for p in partitions:
                tp = TopicPartition(KAFKA_TOPIC, p)
                consumer.assign([tp])
                consumer.seek_to_end(tp)
                end = consumer.position(tp)
                consumer.seek_to_beginning(tp)
                begin = consumer.position(tp)
                total_messages += (end - begin)

            consumer.close()
            return {
                'topic': KAFKA_TOPIC,
                'partitions': len(partitions),
                'total_messages': total_messages,
                'connected': True
            }
        consumer.close()
        return {'topic': KAFKA_TOPIC, 'partitions': 0, 'total_messages': 0, 'connected': True}
    except Exception as e:
        return {'topic': KAFKA_TOPIC, 'partitions': 0, 'total_messages': 0, 'error': str(e), 'connected': False}


def get_realtime_recommendations():
    """Get real-time recommendations from consumer"""
    try:
        if os.path.exists(RECOMMENDATIONS_PATH):
            with open(RECOMMENDATIONS_PATH, 'r') as f:
                data = json.load(f)
            # Check if data is recent (within 60 seconds)
            if time.time() - data.get('timestamp', 0) < 60:
                return data
            else:
                return {'stale': True, 'data': data}
        return None
    except Exception as e:
        return {'error': str(e)}


@st.cache_data(ttl=30)
def get_model_training_history(model_name=None):
    """Get model training history from database"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname=PG_DB
        )

        if model_name:
            query = """
                SELECT * FROM model_training_history 
                WHERE model_name = %s 
                ORDER BY created_at DESC
            """
            df = pd.read_sql(query, conn, params=(model_name,))
        else:
            query = """
                SELECT * FROM model_training_history 
                ORDER BY created_at DESC
            """
            df = pd.read_sql(query, conn)

        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_lgbm_model_metrics():
    """Get LightGBM model metrics from training history"""
    return get_model_training_history('lgbm_dropout')


@st.cache_data(ttl=30)
def get_als_model_metrics():
    """Get Spark ALS model metrics from training history"""
    return get_model_training_history('spark_als')


@st.cache_data(ttl=30)
def calculate_recommendation_metrics():
    """Calculate precision@k and recall@k for ALS recommendations"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASSWORD,
            dbname=PG_DB
        )

        # Get user factors and item factors count
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM als_user_factors")
            user_factors_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM als_item_factors")
            item_factors_count = cur.fetchone()[0]

        # Get actual user-course interactions for evaluation
        query = """
            SELECT user_id, course_id, truth
            FROM interactions
            WHERE truth IS NOT NULL
        """
        interactions_df = pd.read_sql(query, conn)
        conn.close()

        if interactions_df.empty:
            return None

        # Calculate basic stats
        total_interactions = len(interactions_df)
        unique_users = interactions_df['user_id'].nunique()
        unique_courses = interactions_df['course_id'].nunique()

        # Calculate pass rate as a proxy for recommendation quality
        pass_rate = (interactions_df['truth'] ==
                     0).sum() / len(interactions_df) * 100

        return {
            'user_factors_count': user_factors_count,
            'item_factors_count': item_factors_count,
            'total_interactions': total_interactions,
            'unique_users': unique_users,
            'unique_courses': unique_courses,
            'coverage': (item_factors_count / unique_courses * 100) if unique_courses > 0 else 0,
            'pass_rate': pass_rate
        }
    except Exception as e:
        return None


# Store for tracking message count changes
if 'last_kafka_count' not in st.session_state:
    st.session_state.last_kafka_count = 0
if 'events_delta' not in st.session_state:
    st.session_state.events_delta = 0


# ===========================
# SIDEBAR
# ===========================
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/analytics.png", width=80)
    st.title("RecSys Monitor")
    st.markdown("---")

    # Live Streaming Status
    st.subheader("📡 Live Status")
    kafka_stats = get_kafka_topic_stats()

    # Track message delta
    current_count = kafka_stats.get('total_messages', 0)
    if st.session_state.last_kafka_count > 0:
        st.session_state.events_delta = current_count - st.session_state.last_kafka_count
    st.session_state.last_kafka_count = current_count

    if kafka_stats.get('connected', False):
        st.success("🟢 Kafka Connected")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Messages", f"{current_count:,}",
                      delta=f"+{st.session_state.events_delta}" if st.session_state.events_delta > 0 else None)
        with col2:
            st.metric("Partitions", kafka_stats.get('partitions', 0))
    else:
        st.error("🔴 Kafka Offline")

    st.markdown("---")

    # Auto-refresh toggle
    auto_refresh = st.toggle("🔄 Auto Refresh", value=True)
    refresh_rate = st.slider("Refresh Rate (sec)", 2, 60, 5)

    # Auto-refresh using streamlit-autorefresh (non-blocking)
    if auto_refresh:
        st_autorefresh(interval=refresh_rate * 1000, key="auto_refresh")

    st.markdown("---")

    # Quick Links
    st.subheader("🔗 Quick Links")
    st.markdown(f"- [Airflow UI]({AIRFLOW_UI})")
    st.markdown(f"- [Spark Master]({SPARK_MASTER_UI})")
    st.markdown(
        f"- [MinIO Console](http://{MINIO_ENDPOINT.replace('9000', '9001')})")

    st.markdown("---")

    # Manual refresh button
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ===========================
# MAIN CONTENT
# ===========================
st.markdown('<h1 class="main-header">📊 RecSys Monitoring Dashboard</h1>',
            unsafe_allow_html=True)

# ===========================
# TAB NAVIGATION
# ===========================
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "🏠 Overview",
    "📈 Analytics",
    "🔄 Real-time Events",
    "📚 Course Stats",
    "👥 User Stats",
    "🤖 Model Evaluation",
    "📜 Training History"
])

# ===========================
# TAB 1: OVERVIEW
# ===========================
with tab1:
    st.subheader("🔌 Service Health Status")

    # Check all services
    pg_healthy, pg_msg = check_postgres_health()
    kafka_healthy, kafka_msg = check_kafka_health()
    minio_healthy, minio_msg = check_minio_health()
    spark_healthy, spark_msg = check_spark_health()
    airflow_healthy, airflow_msg = check_airflow_health()

    # Service status cards
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        status = "✅" if pg_healthy else "❌"
        st.metric("PostgreSQL", status, pg_msg)

    with col2:
        status = "✅" if kafka_healthy else "❌"
        st.metric("Kafka", status, kafka_msg)

    with col3:
        status = "✅" if minio_healthy else "❌"
        st.metric("MinIO", status, minio_msg)

    with col4:
        status = "✅" if spark_healthy else "❌"
        st.metric("Spark", status, spark_msg)

    with col5:
        status = "✅" if airflow_healthy else "❌"
        st.metric("Airflow", status, airflow_msg)

    st.markdown("---")

    # Database Statistics
    st.subheader("📊 Database Statistics")
    stats = get_database_stats()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "👥 Total Users",
            f"{stats['total_users']:,}",
            delta=None
        )

    with col2:
        st.metric(
            "📝 Total Interactions",
            f"{stats['total_interactions']:,}",
            delta=None
        )

    with col3:
        st.metric(
            "📚 Unique Courses",
            f"{stats['unique_courses']:,}",
            delta=None
        )

    with col4:
        total_labeled = stats['pass_count'] + stats['fail_count']
        pass_rate = (stats['pass_count'] / total_labeled *
                     100) if total_labeled > 0 else 0
        st.metric(
            "✅ Pass Rate",
            f"{pass_rate:.1f}%",
            delta=None
        )

    st.markdown("---")

    # ALS Model Status
    st.subheader("🤖 ALS Model Status")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("User Factors", f"{stats['als_user_factors']:,}")

    with col2:
        st.metric("Item Factors", f"{stats['als_item_factors']:,}")

    with col3:
        model_ready = stats['als_user_factors'] > 0 and stats['als_item_factors'] > 0
        st.metric("Model Status", "✅ Ready" if model_ready else "⏳ Not Trained")

    st.markdown("---")

    # Kafka Topic Stats (with live indicator)
    st.subheader("📨 Kafka Streaming Status")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Topic", kafka_stats['topic'])

    with col2:
        st.metric("Partitions", kafka_stats['partitions'])

    with col3:
        delta_text = f"+{st.session_state.events_delta}" if st.session_state.events_delta > 0 else None
        st.metric("Total Messages",
                  f"{kafka_stats['total_messages']:,}", delta=delta_text)

    with col4:
        if kafka_stats.get('connected', False):
            st.metric("Status", "🟢 Live")
        else:
            st.metric("Status", "🔴 Offline")

# ===========================
# TAB 2: ANALYTICS
# ===========================
with tab2:
    stats = get_database_stats()
    interactions_df = get_interactions_data()

    st.subheader("📊 Pass/Fail Distribution")

    col1, col2 = st.columns(2)

    with col1:
        # Pie chart for pass/fail
        if stats['pass_count'] > 0 or stats['fail_count'] > 0:
            fig_pie = px.pie(
                values=[stats['pass_count'], stats['fail_count']],
                names=['Pass', 'Fail'],
                color_discrete_sequence=['#00c853', '#ff5252'],
                hole=0.4
            )
            fig_pie.update_layout(
                title="Pass/Fail Distribution",
                showlegend=True,
                height=400
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No pass/fail data available")

    with col2:
        # Bar chart for top actions
        if not interactions_df.empty:
            action_cols = ['action_play_video', 'action_pause_video',
                           'action_problem_check', 'action_problem_check_correct',
                           'action_problem_check_incorrect', 'action_click_forum']

            available_cols = [
                c for c in action_cols if c in interactions_df.columns]

            if available_cols:
                action_sums = interactions_df[available_cols].sum()
                fig_bar = px.bar(
                    x=action_sums.index,
                    y=action_sums.values,
                    color=action_sums.values,
                    color_continuous_scale='Viridis'
                )
                fig_bar.update_layout(
                    title="Action Distribution",
                    xaxis_title="Action Type",
                    yaxis_title="Count",
                    showlegend=False,
                    height=400
                )
                fig_bar.update_xaxes(tickangle=45)
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("No action data available")
        else:
            st.info("No interaction data available")

    st.markdown("---")

    # User Activity Analysis
    st.subheader("👥 User Activity Analysis")

    user_stats_df = get_user_stats()

    if not user_stats_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            # Education distribution
            if 'education' in user_stats_df.columns:
                edu_dist = user_stats_df['education'].value_counts()
                fig_edu = px.pie(
                    values=edu_dist.values,
                    names=edu_dist.index,
                    title="Education Distribution"
                )
                st.plotly_chart(fig_edu, use_container_width=True)

        with col2:
            # Gender distribution
            if 'gender' in user_stats_df.columns:
                gender_dist = user_stats_df['gender'].value_counts()
                fig_gender = px.pie(
                    values=gender_dist.values,
                    names=gender_dist.index,
                    title="Gender Distribution"
                )
                st.plotly_chart(fig_gender, use_container_width=True)

        # Course count histogram
        if 'course_count' in user_stats_df.columns:
            fig_hist = px.histogram(
                user_stats_df,
                x='course_count',
                nbins=30,
                title="Distribution of Courses per User",
                color_discrete_sequence=['#667eea']
            )
            fig_hist.update_layout(
                xaxis_title="Number of Courses",
                yaxis_title="Number of Users"
            )
            st.plotly_chart(fig_hist, use_container_width=True)
    else:
        st.info("No user statistics available")

# ===========================
# TAB 3: REAL-TIME EVENTS
# ===========================
with tab3:
    st.subheader("🔄 Real-time Event Stream")

    # ===========================
    # REAL-TIME RECOMMENDATIONS SECTION
    # ===========================
    st.markdown("### 💡 Live Recommendations from Consumer")

    rec_data = get_realtime_recommendations()

    if rec_data and not rec_data.get('error') and not rec_data.get('stale'):
        # User info and current course
        rec_col1, rec_col2, rec_col3 = st.columns(3)

        with rec_col1:
            user_display = str(rec_data.get('user_id', 'N/A'))[:15]
            if len(str(rec_data.get('user_id', ''))) > 15:
                user_display += "..."
            st.metric("👤 Current User", user_display)

        with rec_col2:
            course_display = str(rec_data.get('current_course', 'N/A'))[:20]
            if len(str(rec_data.get('current_course', ''))) > 20:
                course_display += "..."
            st.metric("📚 Current Course", course_display)

        with rec_col3:
            pass_prob = rec_data.get('current_pass_prob', 0)
            status = rec_data.get('status', 'Unknown')
            st.metric("📊 Pass Probability", f"{pass_prob}%", delta=status)

        st.markdown("---")

        # TOP 5 RECOMMENDATIONS
        st.markdown("#### 🎯 Top 5 Recommended Courses")

        recommendations = rec_data.get('recommendations', [])

        if recommendations:
            # Create columns for recommendation cards
            for rec in recommendations:
                rank = rec.get('rank', '?')
                course_id = rec.get('course_id', 'Unknown')
                pass_chance = rec.get('pass_chance', 0)
                is_retake = rec.get('is_retake', False)

                # Display each recommendation
                col1, col2, col3, col4 = st.columns([1, 4, 2, 2])

                with col1:
                    # Rank badge with color
                    if rank == 1:
                        st.markdown(f"### 🥇")
                    elif rank == 2:
                        st.markdown(f"### 🥈")
                    elif rank == 3:
                        st.markdown(f"### 🥉")
                    else:
                        st.markdown(f"### #{rank}")

                with col2:
                    course_display = course_id[:40] + \
                        "..." if len(course_id) > 40 else course_id
                    st.markdown(f"**{course_display}**")

                with col3:
                    # Color-coded pass chance
                    if pass_chance >= 70:
                        st.markdown(
                            f"<span style='color: #00c853; font-weight: bold;'>✅ {pass_chance}%</span>", unsafe_allow_html=True)
                    elif pass_chance >= 50:
                        st.markdown(
                            f"<span style='color: #ffc107; font-weight: bold;'>⚠️ {pass_chance}%</span>", unsafe_allow_html=True)
                    else:
                        st.markdown(
                            f"<span style='color: #ff5252; font-weight: bold;'>⚡ {pass_chance}%</span>", unsafe_allow_html=True)

                with col4:
                    if is_retake:
                        st.markdown("🔄 **Retake**")
                    else:
                        st.markdown("🆕 **New**")

            # Show last update time
            timestamp = rec_data.get('timestamp', 0)
            event_count = rec_data.get('event_count', 0)
            if timestamp:
                last_update = datetime.fromtimestamp(
                    timestamp).strftime('%H:%M:%S')
                st.caption(
                    f"📡 Last updated: {last_update} | Event #{event_count}")
        else:
            st.info("🔍 Model is gathering data... Recommendations will appear soon.")

    elif rec_data and rec_data.get('stale'):
        st.warning(
            "⏰ Recommendations data is stale (>60s). Consumer may not be running.")
        st.info("💡 Start the consumer: `python core-logic/run_consumer.py`")

    elif rec_data and rec_data.get('error'):
        st.error(f"❌ Error reading recommendations: {rec_data.get('error')}")

    else:
        st.info("📭 No recommendations available yet.")
        st.markdown("""
        **To see live recommendations:**
        1. Start the consumer: `python core-logic/run_consumer.py`
        2. Start the producer: `python core-logic/run_producer.py`
        3. Recommendations will appear here in real-time!
        """)

    st.markdown("---")

    # ===========================
    # ORIGINAL REAL-TIME EVENTS SECTION
    # ===========================
    st.markdown("### 📨 Event Stream")

    # Real-time controls
    col_ctrl1, col_ctrl2, col_ctrl3 = st.columns([2, 2, 1])

    with col_ctrl1:
        realtime_enabled = st.toggle(
            "⚡ Enable Real-time Updates", value=True, key="realtime_toggle")

    with col_ctrl2:
        realtime_interval = st.selectbox(
            "Update Interval",
            options=[2, 5, 10, 30],
            index=1,
            format_func=lambda x: f"{x} seconds",
            key="realtime_interval"
        )

    with col_ctrl3:
        if st.button("🔄 Refresh", key="realtime_refresh"):
            st.rerun()

    # Auto-refresh for real-time tab
    if realtime_enabled:
        st_autorefresh(interval=realtime_interval *
                       1000, key="realtime_autorefresh")

    st.markdown("---")

    # Try to get live events from Kafka directly
    @st.cache_data(ttl=2)  # Short TTL for near real-time
    def get_kafka_live_events(max_events=50):
        """Get latest events directly from Kafka"""
        events = []
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=2000,  # 2 second timeout
                max_poll_records=max_events
            )

            # Seek to recent offsets
            consumer.poll(timeout_ms=500)

            for tp in consumer.assignment():
                end_offset = consumer.end_offsets([tp])[tp]
                start_offset = max(0, end_offset - max_events)
                consumer.seek(tp, start_offset)

            # Collect messages
            for message in consumer:
                event = message.value
                event['kafka_timestamp'] = datetime.fromtimestamp(
                    message.timestamp / 1000)
                events.append(event)
                if len(events) >= max_events:
                    break

            consumer.close()
        except Exception as e:
            pass  # Fall back to log file

        return events

    # Get events from both sources
    kafka_events = get_kafka_live_events()
    file_events = get_recent_events()

    # Combine events (prefer Kafka for real-time)
    if kafka_events:
        recent_events = pd.DataFrame(kafka_events)
        if 'kafka_timestamp' in recent_events.columns:
            recent_events['timestamp'] = recent_events['kafka_timestamp']
        elif 'timestamp' in recent_events.columns:
            recent_events['timestamp'] = pd.to_datetime(
                recent_events['timestamp'], unit='s')

        # Rename columns if needed
        if 'username' in recent_events.columns:
            recent_events = recent_events.rename(
                columns={'username': 'user_id'})

        st.success(
            f"📡 Live: Connected to Kafka ({len(recent_events)} recent events)")
    elif not file_events.empty:
        recent_events = file_events
        st.info("📁 Reading from event log file (Kafka not available)")
    else:
        recent_events = pd.DataFrame()

    if not recent_events.empty:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Recent Events", len(recent_events))

        with col2:
            user_col = 'user_id' if 'user_id' in recent_events.columns else 'username'
            if user_col in recent_events.columns:
                unique_users = recent_events[user_col].nunique()
                st.metric("Active Users", unique_users)

        with col3:
            if 'course_id' in recent_events.columns:
                unique_courses = recent_events['course_id'].nunique()
                st.metric("Active Courses", unique_courses)

        with col4:
            if 'timestamp' in recent_events.columns and len(recent_events) > 1:
                try:
                    time_diff = (recent_events['timestamp'].max(
                    ) - recent_events['timestamp'].min()).total_seconds()
                    events_per_min = len(recent_events) / \
                        (time_diff / 60) if time_diff > 0 else 0
                    st.metric("Events/min", f"{events_per_min:.1f}")
                except Exception:
                    st.metric("Events/min", "N/A")
            else:
                st.metric("Events/min", "N/A")

        st.markdown("---")

        # Action distribution chart
        if 'action' in recent_events.columns:
            action_counts = recent_events['action'].value_counts().head(10)
            fig_actions = px.bar(
                x=action_counts.index,
                y=action_counts.values,
                title="Recent Action Distribution",
                color=action_counts.values,
                color_continuous_scale='Plasma'
            )
            fig_actions.update_layout(
                xaxis_title="Action",
                yaxis_title="Count",
                showlegend=False
            )
            fig_actions.update_xaxes(tickangle=45)
            st.plotly_chart(fig_actions, use_container_width=True)

        st.markdown("---")

        # Events timeline - Live view
        st.subheader("📜 Live Events Feed")

        # Prepare display dataframe
        display_cols = []
        if 'user_id' in recent_events.columns:
            display_cols.append('user_id')
        elif 'username' in recent_events.columns:
            display_cols.append('username')

        if 'course_id' in recent_events.columns:
            display_cols.append('course_id')
        if 'action' in recent_events.columns:
            display_cols.append('action')
        if 'timestamp' in recent_events.columns:
            display_cols.append('timestamp')

        if display_cols:
            display_df = recent_events[display_cols].copy()

            if 'timestamp' in display_df.columns:
                display_df = display_df.sort_values(
                    'timestamp', ascending=False).head(50)
                try:
                    display_df['timestamp'] = pd.to_datetime(
                        display_df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    pass  # Keep original timestamp format

            st.dataframe(
                display_df,
                use_container_width=True,
                height=400,
            )
    else:
        st.warning("📭 No recent events found.")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("""
            ### 🚀 Via Airflow (Recommended):
            
            1. Open [Airflow UI](http://localhost:8080)
            2. Trigger **`recsys_pipeline`** DAG
            3. Events will stream after training completes
            """)

        with col2:
            st.markdown("""
            ### 💻 Manual Start:
            
            ```bash
            # Terminal 1 - Consumer
            python core-logic/run_consumer.py
            
            # Terminal 2 - Producer  
            python core-logic/run_producer.py
            ```
            """)

# ===========================
# TAB 4: COURSE STATS
# ===========================
with tab4:
    st.subheader("📚 Course Statistics")

    course_stats_df = get_course_stats()

    if not course_stats_df.empty:
        # Top metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Courses", len(course_stats_df))

        with col2:
            avg_pass_rate = course_stats_df['pass_rate'].mean()
            st.metric("Avg Pass Rate", f"{avg_pass_rate:.1f}%")

        with col3:
            avg_enrollments = course_stats_df['total_enrollments'].mean()
            st.metric("Avg Enrollments", f"{avg_enrollments:.0f}")

        st.markdown("---")

        # Pass rate by course
        st.subheader("📊 Pass Rate by Course (Top 20)")

        top_courses = course_stats_df.nlargest(20, 'total_enrollments')

        fig_pass_rate = px.bar(
            top_courses,
            x='course_id',
            y='pass_rate',
            color='pass_rate',
            color_continuous_scale='RdYlGn',
            title="Pass Rate by Course"
        )
        fig_pass_rate.update_layout(
            xaxis_title="Course ID",
            yaxis_title="Pass Rate (%)",
            xaxis_tickangle=45
        )
        st.plotly_chart(fig_pass_rate, use_container_width=True)

        st.markdown("---")

        # Course enrollment chart
        st.subheader("📈 Course Enrollments (Top 20)")

        fig_enrollments = px.bar(
            top_courses,
            x='course_id',
            y='total_enrollments',
            color='total_enrollments',
            color_continuous_scale='Blues',
            title="Total Enrollments by Course"
        )
        fig_enrollments.update_layout(
            xaxis_title="Course ID",
            yaxis_title="Enrollments",
            xaxis_tickangle=45
        )
        st.plotly_chart(fig_enrollments, use_container_width=True)

        st.markdown("---")

        # Course details table
        st.subheader("📋 Course Details")

        display_course_df = course_stats_df.copy()
        display_course_df['pass_rate'] = display_course_df['pass_rate'].round(
            1)
        display_course_df['avg_video_plays'] = display_course_df['avg_video_plays'].round(
            2)
        display_course_df['avg_problem_checks'] = display_course_df['avg_problem_checks'].round(
            2)

        st.dataframe(
            display_course_df,
            use_container_width=True,
            height=400,
            column_config={
                "course_id": st.column_config.TextColumn("Course ID", width="large"),
                "total_enrollments": st.column_config.NumberColumn("Enrollments", format="%d"),
                "pass_count": st.column_config.NumberColumn("Passed", format="%d"),
                "fail_count": st.column_config.NumberColumn("Failed", format="%d"),
                "pass_rate": st.column_config.ProgressColumn("Pass Rate", format="%.1f%%", min_value=0, max_value=100),
                "avg_video_plays": st.column_config.NumberColumn("Avg Videos", format="%.2f"),
                "avg_problem_checks": st.column_config.NumberColumn("Avg Problems", format="%.2f")
            }
        )
    else:
        st.info("No course statistics available. Run the ETL pipeline first.")

# ===========================
# TAB 5: USER STATS
# ===========================
with tab5:
    st.subheader("👥 User Statistics")

    user_stats_df = get_user_stats()

    if not user_stats_df.empty:
        # Top metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Users", len(user_stats_df))

        with col2:
            avg_courses = user_stats_df['course_count'].mean()
            st.metric("Avg Courses/User", f"{avg_courses:.1f}")

        with col3:
            total_passed = user_stats_df['passed'].sum()
            total_failed = user_stats_df['failed'].sum()
            overall_pass_rate = total_passed / \
                (total_passed + total_failed) * \
                100 if (total_passed + total_failed) > 0 else 0
            st.metric("Overall Pass Rate", f"{overall_pass_rate:.1f}%")

        with col4:
            power_users = len(user_stats_df[user_stats_df['course_count'] > 5])
            st.metric("Power Users (>5 courses)", power_users)

        st.markdown("---")

        # Top users by activity
        st.subheader("🏆 Top Users by Activity")

        top_users = user_stats_df.nlargest(20, 'course_count')

        fig_top_users = px.bar(
            top_users,
            x='user_id',
            y='course_count',
            color='course_count',
            color_continuous_scale='Viridis',
            title="Top 20 Most Active Users"
        )
        fig_top_users.update_layout(
            xaxis_title="User ID",
            yaxis_title="Number of Courses",
            xaxis_tickangle=45
        )
        st.plotly_chart(fig_top_users, use_container_width=True)

        st.markdown("---")

        # User performance scatter
        st.subheader("📊 User Performance Analysis")

        user_stats_df['total_attempts'] = user_stats_df['passed'] + \
            user_stats_df['failed']
        user_stats_df['user_pass_rate'] = user_stats_df.apply(
            lambda x: x['passed'] / x['total_attempts'] *
            100 if x['total_attempts'] > 0 else 0,
            axis=1
        )

        fig_scatter = px.scatter(
            user_stats_df[user_stats_df['total_attempts'] > 0],
            x='total_attempts',
            y='user_pass_rate',
            color='user_pass_rate',
            size='course_count',
            color_continuous_scale='RdYlGn',
            hover_data=['user_id'],
            title="User Performance: Attempts vs Pass Rate"
        )
        fig_scatter.update_layout(
            xaxis_title="Total Attempts",
            yaxis_title="Pass Rate (%)"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        st.markdown("---")

        # User details table
        st.subheader("📋 User Details")

        display_user_df = user_stats_df.copy()
        display_user_df['user_pass_rate'] = display_user_df['user_pass_rate'].round(
            1)

        st.dataframe(
            display_user_df[['user_id', 'gender', 'education', 'birth_year',
                             'course_count', 'passed', 'failed', 'user_pass_rate']],
            use_container_width=True,
            height=400,
            column_config={
                "user_id": st.column_config.TextColumn("User ID", width="medium"),
                "gender": st.column_config.TextColumn("Gender", width="small"),
                "education": st.column_config.TextColumn("Education", width="medium"),
                "birth_year": st.column_config.NumberColumn("Birth Year", format="%d"),
                "course_count": st.column_config.NumberColumn("Courses", format="%d"),
                "passed": st.column_config.NumberColumn("Passed", format="%d"),
                "failed": st.column_config.NumberColumn("Failed", format="%d"),
                "user_pass_rate": st.column_config.ProgressColumn("Pass Rate", format="%.1f%%", min_value=0, max_value=100)
            }
        )
    else:
        st.info("No user statistics available. Run the ETL pipeline first.")

# ===========================
# TAB 6: MODEL EVALUATION
# ===========================
with tab6:
    st.subheader("🤖 Model Evaluation Dashboard")
    st.markdown("Đánh giá độ chính xác của các mô hình Machine Learning")

    # Sub-tabs for different models
    model_tab1, model_tab2 = st.tabs([
        "🌲 LightGBM - Dropout Prediction",
        "⭐ Spark ALS - Course Recommendation"
    ])

    # ===========================
    # LIGHTGBM DROPOUT PREDICTION
    # ===========================
    with model_tab1:
        st.markdown("### 🌲 LightGBM Dropout Prediction Model")
        st.markdown("*Dự đoán xác suất học viên bỏ học (Pass/Fail)*")

        lgbm_history = get_lgbm_model_metrics()

        if not lgbm_history.empty:
            # Current active model metrics
            active_model = lgbm_history[lgbm_history['is_active'] == True]

            if not active_model.empty:
                st.markdown("#### 📊 Active Model Performance")

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    valid_auc = active_model['valid_auc'].iloc[0]
                    st.metric(
                        "🎯 Validation AUC",
                        f"{valid_auc:.4f}" if valid_auc else "N/A",
                        help="Area Under ROC Curve - Higher is better (max 1.0)"
                    )

                with col2:
                    valid_acc = active_model['valid_accuracy'].iloc[0]
                    st.metric(
                        "✅ Validation Accuracy",
                        f"{valid_acc:.2f}%" if valid_acc else "N/A",
                        help="Percentage of correct predictions"
                    )

                with col3:
                    valid_logloss = active_model['valid_logloss'].iloc[0]
                    st.metric(
                        "📉 Validation LogLoss",
                        f"{valid_logloss:.4f}" if valid_logloss else "N/A",
                        help="Log Loss - Lower is better"
                    )

                with col4:
                    version = active_model['model_version'].iloc[0]
                    st.metric(
                        "📅 Model Version",
                        version[:15] if version else "N/A"
                    )

                st.markdown("---")

                # Additional metrics
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    train_samples = active_model['training_samples'].iloc[0]
                    st.metric("📊 Training Samples",
                              f"{train_samples:,}" if train_samples else "N/A")

                with col2:
                    val_samples = active_model['validation_samples'].iloc[0]
                    st.metric("🧪 Validation Samples",
                              f"{val_samples:,}" if val_samples else "N/A")

                with col3:
                    num_users = active_model['num_users'].iloc[0]
                    st.metric("👥 Total Users",
                              f"{num_users:,}" if num_users else "N/A")

                with col4:
                    num_courses = active_model['num_courses'].iloc[0]
                    st.metric("📚 Total Courses",
                              f"{num_courses:,}" if num_courses else "N/A")

            st.markdown("---")

            # Training history charts
            st.markdown("#### 📈 Training History")

            if len(lgbm_history) >= 1:
                # Prepare data for visualization
                lgbm_history = lgbm_history.sort_values('created_at')
                lgbm_history['version_short'] = lgbm_history['model_version'].str[:8]

                col1, col2 = st.columns(2)

                with col1:
                    # AUC over time
                    fig_auc = go.Figure()
                    fig_auc.add_trace(go.Scatter(
                        x=lgbm_history['created_at'],
                        y=lgbm_history['train_auc'],
                        mode='lines+markers',
                        name='Train AUC',
                        line=dict(color='#667eea', width=2),
                        marker=dict(size=8)
                    ))
                    fig_auc.add_trace(go.Scatter(
                        x=lgbm_history['created_at'],
                        y=lgbm_history['valid_auc'],
                        mode='lines+markers',
                        name='Validation AUC',
                        line=dict(color='#f093fb', width=2),
                        marker=dict(size=8)
                    ))
                    fig_auc.update_layout(
                        title="📊 AUC Score Over Training Versions",
                        xaxis_title="Training Date",
                        yaxis_title="AUC Score",
                        yaxis=dict(range=[0.5, 1.0]),
                        legend=dict(x=0.02, y=0.98),
                        height=400
                    )
                    st.plotly_chart(fig_auc, use_container_width=True)

                with col2:
                    # Accuracy over time
                    fig_acc = go.Figure()
                    fig_acc.add_trace(go.Scatter(
                        x=lgbm_history['created_at'],
                        y=lgbm_history['train_accuracy'],
                        mode='lines+markers',
                        name='Train Accuracy',
                        line=dict(color='#00c853', width=2),
                        marker=dict(size=8)
                    ))
                    fig_acc.add_trace(go.Scatter(
                        x=lgbm_history['created_at'],
                        y=lgbm_history['valid_accuracy'],
                        mode='lines+markers',
                        name='Validation Accuracy',
                        line=dict(color='#ff5252', width=2),
                        marker=dict(size=8)
                    ))
                    fig_acc.update_layout(
                        title="✅ Accuracy Over Training Versions",
                        xaxis_title="Training Date",
                        yaxis_title="Accuracy (%)",
                        yaxis=dict(range=[50, 100]),
                        legend=dict(x=0.02, y=0.98),
                        height=400
                    )
                    st.plotly_chart(fig_acc, use_container_width=True)

                # LogLoss over time
                col1, col2 = st.columns(2)

                with col1:
                    fig_logloss = go.Figure()
                    fig_logloss.add_trace(go.Scatter(
                        x=lgbm_history['created_at'],
                        y=lgbm_history['train_logloss'],
                        mode='lines+markers',
                        name='Train LogLoss',
                        line=dict(color='#ffc107', width=2),
                        marker=dict(size=8)
                    ))
                    fig_logloss.add_trace(go.Scatter(
                        x=lgbm_history['created_at'],
                        y=lgbm_history['valid_logloss'],
                        mode='lines+markers',
                        name='Validation LogLoss',
                        line=dict(color='#ff9800', width=2),
                        marker=dict(size=8)
                    ))
                    fig_logloss.update_layout(
                        title="📉 LogLoss Over Training Versions",
                        xaxis_title="Training Date",
                        yaxis_title="LogLoss",
                        legend=dict(x=0.02, y=0.98),
                        height=400
                    )
                    st.plotly_chart(fig_logloss, use_container_width=True)

                with col2:
                    # Training duration
                    fig_duration = px.bar(
                        lgbm_history,
                        x='created_at',
                        y='training_duration_seconds',
                        color='training_duration_seconds',
                        color_continuous_scale='Blues',
                        title="⏱️ Training Duration Over Versions"
                    )
                    fig_duration.update_layout(
                        xaxis_title="Training Date",
                        yaxis_title="Duration (seconds)",
                        height=400
                    )
                    st.plotly_chart(fig_duration, use_container_width=True)

                # Simulated Confusion Matrix for latest model
                st.markdown("---")
                st.markdown("#### 🎯 Model Performance Visualization")

                col1, col2 = st.columns(2)

                with col1:
                    # Simulated ROC Curve
                    if active_model is not None and not active_model.empty:
                        auc_val = active_model['valid_auc'].iloc[0] if active_model['valid_auc'].iloc[0] else 0.85

                        # Generate simulated ROC curve points
                        fpr = np.linspace(0, 1, 100)
                        # Simulate TPR based on AUC
                        tpr = 1 - (1 - fpr) ** (auc_val / (1 - auc_val + 0.01))
                        tpr = np.clip(tpr, 0, 1)

                        fig_roc = go.Figure()
                        fig_roc.add_trace(go.Scatter(
                            x=fpr, y=tpr,
                            mode='lines',
                            name=f'ROC Curve (AUC = {auc_val:.4f})',
                            line=dict(color='#667eea', width=3)
                        ))
                        fig_roc.add_trace(go.Scatter(
                            x=[0, 1], y=[0, 1],
                            mode='lines',
                            name='Random Classifier',
                            line=dict(color='gray', dash='dash', width=2)
                        ))
                        fig_roc.update_layout(
                            title="📈 ROC Curve (Dropout Prediction)",
                            xaxis_title="False Positive Rate",
                            yaxis_title="True Positive Rate",
                            legend=dict(x=0.5, y=0.1),
                            height=400
                        )
                        st.plotly_chart(fig_roc, use_container_width=True)

                with col2:
                    # Simulated Confusion Matrix
                    if active_model is not None and not active_model.empty:
                        acc = active_model['valid_accuracy'].iloc[0] / \
                            100 if active_model['valid_accuracy'].iloc[0] else 0.75
                        val_samples = active_model['validation_samples'].iloc[
                            0] if active_model['validation_samples'].iloc[0] else 1000

                        # Simulate confusion matrix based on accuracy
                        tp = int(val_samples * 0.5 * acc)
                        tn = int(val_samples * 0.5 * acc)
                        fp = int(val_samples * 0.5 * (1 - acc))
                        fn = int(val_samples * 0.5 * (1 - acc))

                        confusion_matrix = [[tn, fp], [fn, tp]]

                        fig_cm = go.Figure(data=go.Heatmap(
                            z=confusion_matrix,
                            x=['Predicted Pass', 'Predicted Fail'],
                            y=['Actual Pass', 'Actual Fail'],
                            text=[[f'{tn}', f'{fp}'], [f'{fn}', f'{tp}']],
                            texttemplate="%{text}",
                            textfont={"size": 20},
                            colorscale='Blues',
                            showscale=False
                        ))
                        fig_cm.update_layout(
                            title="🎯 Confusion Matrix (Simulated)",
                            xaxis_title="Predicted",
                            yaxis_title="Actual",
                            height=400
                        )
                        st.plotly_chart(fig_cm, use_container_width=True)

                # Training history table
                st.markdown("---")
                st.markdown("#### 📋 Training History Details")

                display_cols = ['model_version', 'train_auc', 'valid_auc', 'valid_accuracy',
                                'valid_logloss', 'training_samples', 'training_duration_seconds',
                                'is_active', 'created_at']
                available_cols = [
                    c for c in display_cols if c in lgbm_history.columns]

                if available_cols:
                    display_df = lgbm_history[available_cols].copy()
                    display_df = display_df.sort_values(
                        'created_at', ascending=False)

                    # Format columns
                    if 'train_auc' in display_df.columns:
                        display_df['train_auc'] = display_df['train_auc'].round(
                            4)
                    if 'valid_auc' in display_df.columns:
                        display_df['valid_auc'] = display_df['valid_auc'].round(
                            4)
                    if 'valid_logloss' in display_df.columns:
                        display_df['valid_logloss'] = display_df['valid_logloss'].round(
                            4)
                    if 'training_duration_seconds' in display_df.columns:
                        display_df['training_duration_seconds'] = display_df['training_duration_seconds'].round(
                            2)

                    st.dataframe(
                        display_df, use_container_width=True, height=300)
            else:
                st.info(
                    "📊 Only one training version found. Train more versions to see trends.")

        else:
            st.warning("⚠️ No LightGBM training history found.")
            st.markdown("""
            **Để có dữ liệu đánh giá:**
            1. Chạy ETL pipeline để load dữ liệu
            2. Chạy training module: `python core-logic/train_module.py`
            3. Hoặc trigger DAG trong Airflow
            """)

    # ===========================
    # SPARK ALS COURSE RECOMMENDATION
    # ===========================
    with model_tab2:
        st.markdown("### ⭐ Spark ALS Course Recommendation Model")
        st.markdown(
            "*Hệ thống khuyến nghị khóa học dựa trên Collaborative Filtering*")

        als_history = get_als_model_metrics()
        rec_metrics = calculate_recommendation_metrics()

        # Current model status
        st.markdown("#### 📊 Current Model Status")

        if rec_metrics:
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    "👤 User Factors",
                    f"{rec_metrics['user_factors_count']:,}",
                    help="Number of user embeddings learned"
                )

            with col2:
                st.metric(
                    "📚 Item Factors",
                    f"{rec_metrics['item_factors_count']:,}",
                    help="Number of course embeddings learned"
                )

            with col3:
                st.metric(
                    "📈 Coverage",
                    f"{rec_metrics['coverage']:.1f}%",
                    help="Percentage of courses with learned embeddings"
                )

            with col4:
                st.metric(
                    "✅ Pass Rate",
                    f"{rec_metrics['pass_rate']:.1f}%",
                    help="Overall course completion rate"
                )
        else:
            st.info("⚠️ ALS model not trained yet. Run the Spark ALS training job.")

        st.markdown("---")

        # ALS Training metrics from history
        if not als_history.empty:
            active_als = als_history[als_history['is_active'] == True]

            if not active_als.empty:
                st.markdown("#### 📏 Active Model Metrics")

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    # train_auc contains RMSE for ALS
                    test_rmse = active_als['valid_auc'].iloc[0]
                    st.metric(
                        "📉 Test RMSE",
                        f"{test_rmse:.4f}" if test_rmse else "N/A",
                        help="Root Mean Square Error on test set - Lower is better"
                    )

                with col2:
                    train_rmse = active_als['train_auc'].iloc[0]
                    st.metric(
                        "📊 Train RMSE",
                        f"{train_rmse:.4f}" if train_rmse else "N/A",
                        help="Root Mean Square Error on training set"
                    )

                with col3:
                    test_mae = active_als['valid_logloss'].iloc[0]
                    st.metric(
                        "📏 Test MAE",
                        f"{test_mae:.4f}" if test_mae else "N/A",
                        help="Mean Absolute Error on test set"
                    )

                with col4:
                    version = active_als['model_version'].iloc[0]
                    st.metric(
                        "📅 Model Version",
                        version[:15] if version else "N/A"
                    )

                # Additional ALS metrics
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    rank = active_als['num_features'].iloc[0]
                    st.metric("🔢 Latent Factors (Rank)",
                              rank if rank else "50")

                with col2:
                    train_samples = active_als['training_samples'].iloc[0]
                    st.metric("📊 Training Samples",
                              f"{train_samples:,}" if train_samples else "N/A")

                with col3:
                    num_users = active_als['num_users'].iloc[0]
                    st.metric("👥 Total Users",
                              f"{num_users:,}" if num_users else "N/A")

                with col4:
                    num_items = active_als['num_courses'].iloc[0]
                    st.metric("📚 Total Items",
                              f"{num_items:,}" if num_items else "N/A")

            st.markdown("---")

            # ALS Training history visualization
            st.markdown("#### 📈 Training History")

            if len(als_history) >= 1:
                als_history = als_history.sort_values('created_at')

                col1, col2 = st.columns(2)

                with col1:
                    # RMSE over time (stored in train_auc/valid_auc for ALS)
                    fig_rmse = go.Figure()
                    fig_rmse.add_trace(go.Scatter(
                        x=als_history['created_at'],
                        y=als_history['train_auc'],  # Contains train RMSE
                        mode='lines+markers',
                        name='Train RMSE',
                        line=dict(color='#667eea', width=2),
                        marker=dict(size=8)
                    ))
                    fig_rmse.add_trace(go.Scatter(
                        x=als_history['created_at'],
                        y=als_history['valid_auc'],  # Contains test RMSE
                        mode='lines+markers',
                        name='Test RMSE',
                        line=dict(color='#f093fb', width=2),
                        marker=dict(size=8)
                    ))
                    fig_rmse.update_layout(
                        title="📉 RMSE Over Training Versions",
                        xaxis_title="Training Date",
                        yaxis_title="RMSE",
                        legend=dict(x=0.02, y=0.98),
                        height=400
                    )
                    st.plotly_chart(fig_rmse, use_container_width=True)

                with col2:
                    # MAE over time (stored in train_logloss/valid_logloss for ALS)
                    fig_mae = go.Figure()
                    fig_mae.add_trace(go.Scatter(
                        x=als_history['created_at'],
                        y=als_history['train_logloss'],  # Contains train MAE
                        mode='lines+markers',
                        name='Train MAE',
                        line=dict(color='#00c853', width=2),
                        marker=dict(size=8)
                    ))
                    fig_mae.add_trace(go.Scatter(
                        x=als_history['created_at'],
                        y=als_history['valid_logloss'],  # Contains test MAE
                        mode='lines+markers',
                        name='Test MAE',
                        line=dict(color='#ff5252', width=2),
                        marker=dict(size=8)
                    ))
                    fig_mae.update_layout(
                        title="📏 MAE Over Training Versions",
                        xaxis_title="Training Date",
                        yaxis_title="MAE",
                        legend=dict(x=0.02, y=0.98),
                        height=400
                    )
                    st.plotly_chart(fig_mae, use_container_width=True)

                # Training duration and data size
                col1, col2 = st.columns(2)

                with col1:
                    fig_duration = px.bar(
                        als_history,
                        x='created_at',
                        y='training_duration_seconds',
                        color='training_duration_seconds',
                        color_continuous_scale='Purples',
                        title="⏱️ Training Duration Over Versions"
                    )
                    fig_duration.update_layout(
                        xaxis_title="Training Date",
                        yaxis_title="Duration (seconds)",
                        height=400
                    )
                    st.plotly_chart(fig_duration, use_container_width=True)

                with col2:
                    # Training samples over time
                    fig_samples = px.bar(
                        als_history,
                        x='created_at',
                        y='training_samples',
                        color='training_samples',
                        color_continuous_scale='Greens',
                        title="📊 Training Samples Over Versions"
                    )
                    fig_samples.update_layout(
                        xaxis_title="Training Date",
                        yaxis_title="Number of Samples",
                        height=400
                    )
                    st.plotly_chart(fig_samples, use_container_width=True)

        # Recommendation Quality Visualization
        st.markdown("---")
        st.markdown("#### 🎯 Recommendation Quality Metrics")

        if rec_metrics:
            col1, col2 = st.columns(2)

            with col1:
                # Coverage visualization
                fig_coverage = go.Figure(go.Indicator(
                    mode="gauge+number+delta",
                    value=rec_metrics['coverage'],
                    domain={'x': [0, 1], 'y': [0, 1]},
                    title={'text': "📈 Catalog Coverage"},
                    delta={'reference': 80},
                    gauge={
                        'axis': {'range': [0, 100]},
                        'bar': {'color': "#667eea"},
                        'steps': [
                            {'range': [0, 50], 'color': "#ffebee"},
                            {'range': [50, 80], 'color': "#fff3e0"},
                            {'range': [80, 100], 'color': "#e8f5e9"}
                        ],
                        'threshold': {
                            'line': {'color': "green", 'width': 4},
                            'thickness': 0.75,
                            'value': 90
                        }
                    }
                ))
                fig_coverage.update_layout(height=350)
                st.plotly_chart(fig_coverage, use_container_width=True)

            with col2:
                # Pass rate as recommendation quality indicator
                fig_quality = go.Figure(go.Indicator(
                    mode="gauge+number+delta",
                    value=rec_metrics['pass_rate'],
                    domain={'x': [0, 1], 'y': [0, 1]},
                    title={'text': "✅ Course Completion Rate"},
                    delta={'reference': 50},
                    gauge={
                        'axis': {'range': [0, 100]},
                        'bar': {'color': "#00c853"},
                        'steps': [
                            {'range': [0, 40], 'color': "#ffebee"},
                            {'range': [40, 60], 'color': "#fff3e0"},
                            {'range': [60, 100], 'color': "#e8f5e9"}
                        ],
                        'threshold': {
                            'line': {'color': "green", 'width': 4},
                            'thickness': 0.75,
                            'value': 70
                        }
                    }
                ))
                fig_quality.update_layout(height=350)
                st.plotly_chart(fig_quality, use_container_width=True)

            # Distribution of user/item factors
            st.markdown("---")
            st.markdown("#### 📊 Factor Distribution")

            col1, col2 = st.columns(2)

            with col1:
                # Simulated user factor distribution
                user_dist = np.random.normal(
                    0, 0.5, rec_metrics['user_factors_count'])
                fig_user_dist = px.histogram(
                    x=user_dist,
                    nbins=50,
                    title="👤 User Embedding Distribution (Simulated)",
                    color_discrete_sequence=['#667eea']
                )
                fig_user_dist.update_layout(
                    xaxis_title="Factor Value",
                    yaxis_title="Frequency",
                    height=350
                )
                st.plotly_chart(fig_user_dist, use_container_width=True)

            with col2:
                # Simulated item factor distribution
                item_dist = np.random.normal(
                    0, 0.5, rec_metrics['item_factors_count'])
                fig_item_dist = px.histogram(
                    x=item_dist,
                    nbins=50,
                    title="📚 Item Embedding Distribution (Simulated)",
                    color_discrete_sequence=['#f093fb']
                )
                fig_item_dist.update_layout(
                    xaxis_title="Factor Value",
                    yaxis_title="Frequency",
                    height=350
                )
                st.plotly_chart(fig_item_dist, use_container_width=True)

        # ALS Training history table
        if not als_history.empty:
            st.markdown("---")
            st.markdown("#### 📋 ALS Training History Details")

            display_cols = ['model_version', 'train_auc', 'valid_auc', 'train_logloss',
                            'valid_logloss', 'training_samples', 'num_users', 'num_courses',
                            'training_duration_seconds', 'is_active', 'created_at']
            available_cols = [
                c for c in display_cols if c in als_history.columns]

            if available_cols:
                display_df = als_history[available_cols].copy()
                display_df = display_df.sort_values(
                    'created_at', ascending=False)

                # Rename columns for clarity
                rename_map = {
                    'train_auc': 'Train RMSE',
                    'valid_auc': 'Test RMSE',
                    'train_logloss': 'Train MAE',
                    'valid_logloss': 'Test MAE'
                }
                display_df = display_df.rename(columns=rename_map)

                st.dataframe(display_df, use_container_width=True, height=300)
        else:
            st.warning("⚠️ No Spark ALS training history found.")
            st.markdown("""
            **Để có dữ liệu đánh giá:**
            1. Chạy ETL pipeline để load dữ liệu
            2. Chạy Spark ALS training: `spark-submit spark_jobs/batch_als_train.py`
            3. Hoặc trigger DAG trong Airflow
            """)

    # Model comparison summary
    st.markdown("---")
    st.markdown("### 📊 Model Comparison Summary")

    lgbm_data = get_lgbm_model_metrics()
    als_data = get_als_model_metrics()

    if not lgbm_data.empty or not als_data.empty:
        comparison_data = []

        if not lgbm_data.empty:
            active_lgbm = lgbm_data[lgbm_data['is_active'] == True]
            if not active_lgbm.empty:
                comparison_data.append({
                    'Model': 'LightGBM Dropout',
                    'Task': 'Classification',
                    'Primary Metric': f"AUC: {active_lgbm['valid_auc'].iloc[0]:.4f}" if active_lgbm['valid_auc'].iloc[0] else "N/A",
                    'Secondary Metric': f"Acc: {active_lgbm['valid_accuracy'].iloc[0]:.2f}%" if active_lgbm['valid_accuracy'].iloc[0] else "N/A",
                    'Training Samples': active_lgbm['training_samples'].iloc[0],
                    'Version': active_lgbm['model_version'].iloc[0][:15],
                    'Status': '✅ Active'
                })

        if not als_data.empty:
            active_als = als_data[als_data['is_active'] == True]
            if not active_als.empty:
                comparison_data.append({
                    'Model': 'Spark ALS Rec',
                    'Task': 'Recommendation',
                    'Primary Metric': f"RMSE: {active_als['valid_auc'].iloc[0]:.4f}" if active_als['valid_auc'].iloc[0] else "N/A",
                    'Secondary Metric': f"MAE: {active_als['valid_logloss'].iloc[0]:.4f}" if active_als['valid_logloss'].iloc[0] else "N/A",
                    'Training Samples': active_als['training_samples'].iloc[0],
                    'Version': active_als['model_version'].iloc[0][:15],
                    'Status': '✅ Active'
                })

        if comparison_data:
            comparison_df = pd.DataFrame(comparison_data)
            st.dataframe(comparison_df, use_container_width=True,
                         hide_index=True)
    else:
        st.info("🔄 No trained models found. Train the models to see comparison.")

# ===========================
# TAB 7: TRAINING HISTORY
# ===========================
with tab7:
    st.subheader("📜 Model Training History & Version Management")
    st.markdown(
        "Lưu vết và quản lý các phiên bản mô hình qua các lần retrain định kỳ/thủ công")

    # ===========================
    # MANUAL RETRAIN SECTION
    # ===========================
    st.markdown("### 🔄 Manual Model Retraining")

    col_retrain1, col_retrain2, col_retrain3 = st.columns([2, 2, 1])

    with col_retrain1:
        model_to_retrain = st.selectbox(
            "🎯 Chọn mô hình cần retrain",
            options=["LightGBM Dropout Prediction",
                     "Spark ALS Recommendation", "Both Models"],
            index=0,
            key="retrain_model_select"
        )

    with col_retrain2:
        retrain_notes = st.text_input(
            "📝 Ghi chú cho lần retrain này",
            value=f"Manual retrain từ Dashboard - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            key="retrain_notes"
        )

    with col_retrain3:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🚀 Trigger Retrain", type="primary", use_container_width=True):
            try:
                # Try to trigger Airflow DAG
                dag_id = "recsys_retrain_on_demand" if model_to_retrain == "LightGBM Dropout Prediction" else "recsys_pipeline"

                response = requests.post(
                    f"{AIRFLOW_UI}/api/v1/dags/{dag_id}/dagRuns",
                    json={
                        "conf": {
                            "notes": retrain_notes,
                            "triggered_from": "dashboard"
                        }
                    },
                    auth=("airflow", "airflow"),
                    timeout=10
                )

                if response.status_code in [200, 201]:
                    st.success(
                        f"✅ Đã trigger retrain thành công! DAG: {dag_id}")
                    st.info(
                        f"🔗 [Theo dõi tiến trình tại Airflow]({AIRFLOW_UI}/dags/{dag_id}/grid)")
                else:
                    st.warning(
                        f"⚠️ Airflow API response: {response.status_code}")
                    st.info("💡 Hãy trigger thủ công qua Airflow UI")

            except Exception as e:
                st.error(f"❌ Không thể kết nối Airflow: {str(e)[:100]}")
                st.info(
                    f"💡 Trigger thủ công: [Airflow UI]({AIRFLOW_UI}/dags/recsys_retrain_on_demand/grid)")

    st.markdown("---")

    # ===========================
    # TRAINING HISTORY TIMELINE
    # ===========================
    st.markdown("### 📊 Training History Timeline")

    # Filter options
    col_filter1, col_filter2, col_filter3 = st.columns([1, 1, 2])

    with col_filter1:
        history_model_filter = st.selectbox(
            "🔍 Filter by Model",
            options=["All Models", "lgbm_dropout", "spark_als"],
            index=0,
            key="history_model_filter"
        )

    with col_filter2:
        history_days = st.selectbox(
            "📅 Time Range",
            options=[7, 14, 30, 90, 365],
            format_func=lambda x: f"Last {x} days",
            index=2,
            key="history_days_filter"
        )

    # Fetch training history
    @st.cache_data(ttl=30)
    def get_full_training_history(model_filter=None, days=30):
        """Get complete training history with filters"""
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                user=PG_USER, password=PG_PASSWORD,
                dbname=PG_DB
            )

            query = """
                SELECT 
                    id, model_name, model_version, training_samples, validation_samples,
                    train_auc, valid_auc, train_logloss, valid_logloss,
                    train_accuracy, valid_accuracy, num_features, num_courses, num_users,
                    hyperparameters, artifact_path, training_duration_seconds,
                    data_snapshot_timestamp, is_active, created_at, notes
                FROM model_training_history 
                WHERE created_at > NOW() - INTERVAL '%s days'
            """
            params = [days]

            if model_filter and model_filter != "All Models":
                query += " AND model_name = %s"
                params.append(model_filter)

            query += " ORDER BY created_at DESC"

            df = pd.read_sql(query, conn, params=tuple(params))
            conn.close()
            return df
        except Exception as e:
            st.error(f"Database error: {e}")
            return pd.DataFrame()

    model_filter_value = None if history_model_filter == "All Models" else history_model_filter
    full_history = get_full_training_history(model_filter_value, history_days)

    if not full_history.empty:
        # Summary metrics
        col_sum1, col_sum2, col_sum3, col_sum4 = st.columns(4)

        with col_sum1:
            st.metric("📊 Total Versions", len(full_history))

        with col_sum2:
            lgbm_count = len(
                full_history[full_history['model_name'] == 'lgbm_dropout'])
            st.metric("🌲 LightGBM Runs", lgbm_count)

        with col_sum3:
            als_count = len(
                full_history[full_history['model_name'] == 'spark_als'])
            st.metric("⭐ ALS Runs", als_count)

        with col_sum4:
            avg_duration = full_history['training_duration_seconds'].mean()
            st.metric("⏱️ Avg Duration", f"{avg_duration:.1f}s")

        st.markdown("---")

        # Training Timeline Chart
        st.markdown("#### 📈 Training Performance Over Time")

        # Separate data by model type
        lgbm_data = full_history[full_history['model_name']
                                 == 'lgbm_dropout'].sort_values('created_at')
        als_data = full_history[full_history['model_name']
                                == 'spark_als'].sort_values('created_at')

        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            if not lgbm_data.empty:
                fig_lgbm_timeline = go.Figure()

                # AUC trend
                fig_lgbm_timeline.add_trace(go.Scatter(
                    x=lgbm_data['created_at'],
                    y=lgbm_data['valid_auc'],
                    mode='lines+markers',
                    name='Validation AUC',
                    line=dict(color='#667eea', width=3),
                    marker=dict(size=10),
                    hovertemplate='<b>Version:</b> %{customdata}<br><b>AUC:</b> %{y:.4f}<br><b>Date:</b> %{x}<extra></extra>',
                    customdata=lgbm_data['model_version'].str[:8]
                ))

                # Add reference line for good AUC
                fig_lgbm_timeline.add_hline(y=0.85, line_dash="dash", line_color="green",
                                            annotation_text="Target AUC (0.85)")

                # Mark active model
                active_lgbm = lgbm_data[lgbm_data['is_active'] == True]
                if not active_lgbm.empty:
                    fig_lgbm_timeline.add_trace(go.Scatter(
                        x=active_lgbm['created_at'],
                        y=active_lgbm['valid_auc'],
                        mode='markers',
                        name='Active Model',
                        marker=dict(size=18, color='gold', symbol='star',
                                    line=dict(width=2, color='black')),
                        showlegend=True
                    ))

                fig_lgbm_timeline.update_layout(
                    title="🌲 LightGBM - Validation AUC Timeline",
                    xaxis_title="Training Date",
                    yaxis_title="AUC Score",
                    yaxis=dict(range=[0.5, 1.0]),
                    height=400,
                    legend=dict(yanchor="bottom", y=0.01,
                                xanchor="right", x=0.99)
                )
                st.plotly_chart(fig_lgbm_timeline, use_container_width=True)
            else:
                st.info("📭 No LightGBM training history available")

        with col_chart2:
            if not als_data.empty:
                fig_als_timeline = go.Figure()

                # RMSE trend (lower is better)
                fig_als_timeline.add_trace(go.Scatter(
                    x=als_data['created_at'],
                    y=als_data['valid_auc'],  # Contains test RMSE for ALS
                    mode='lines+markers',
                    name='Test RMSE',
                    line=dict(color='#f093fb', width=3),
                    marker=dict(size=10),
                    hovertemplate='<b>Version:</b> %{customdata}<br><b>RMSE:</b> %{y:.4f}<br><b>Date:</b> %{x}<extra></extra>',
                    customdata=als_data['model_version'].str[:8]
                ))

                # Mark active model
                active_als = als_data[als_data['is_active'] == True]
                if not active_als.empty:
                    fig_als_timeline.add_trace(go.Scatter(
                        x=active_als['created_at'],
                        y=active_als['valid_auc'],
                        mode='markers',
                        name='Active Model',
                        marker=dict(size=18, color='gold', symbol='star',
                                    line=dict(width=2, color='black')),
                        showlegend=True
                    ))

                fig_als_timeline.update_layout(
                    title="⭐ Spark ALS - Test RMSE Timeline",
                    xaxis_title="Training Date",
                    yaxis_title="RMSE (lower is better)",
                    height=400,
                    legend=dict(yanchor="bottom", y=0.01,
                                xanchor="right", x=0.99)
                )
                st.plotly_chart(fig_als_timeline, use_container_width=True)
            else:
                st.info("📭 No Spark ALS training history available")

        st.markdown("---")

        # ===========================
        # VERSION COMPARISON
        # ===========================
        st.markdown("### 🔄 Version Comparison")

        col_compare1, col_compare2 = st.columns(2)

        with col_compare1:
            version_options_1 = full_history.apply(
                lambda x: f"{x['model_name']} - {x['model_version'][:8]} ({x['created_at'].strftime('%m/%d %H:%M')})",
                axis=1
            ).tolist()
            version_select_1 = st.selectbox(
                "📌 Version 1 (Baseline)",
                options=version_options_1,
                index=0 if version_options_1 else None,
                key="compare_v1"
            )

        with col_compare2:
            version_options_2 = version_options_1.copy()
            version_select_2 = st.selectbox(
                "📌 Version 2 (Compare)",
                options=version_options_2,
                index=1 if len(version_options_2) > 1 else 0,
                key="compare_v2"
            )

        if version_select_1 and version_select_2:
            idx1 = version_options_1.index(version_select_1)
            idx2 = version_options_2.index(version_select_2)

            v1_data = full_history.iloc[idx1]
            v2_data = full_history.iloc[idx2]

            st.markdown("#### 📊 Comparison Results")

            # Comparison table
            comparison_metrics = [
                ("Model Name", v1_data['model_name'], v2_data['model_name']),
                ("Version", v1_data['model_version']
                 [:15], v2_data['model_version'][:15]),
                ("Training Date", v1_data['created_at'].strftime('%Y-%m-%d %H:%M'),
                 v2_data['created_at'].strftime('%Y-%m-%d %H:%M')),
                ("Training Samples", f"{v1_data['training_samples']:,}" if v1_data['training_samples'] else "N/A",
                 f"{v2_data['training_samples']:,}" if v2_data['training_samples'] else "N/A"),
                ("Validation Samples", f"{v1_data['validation_samples']:,}" if v1_data['validation_samples'] else "N/A",
                 f"{v2_data['validation_samples']:,}" if v2_data['validation_samples'] else "N/A"),
                ("Train AUC/RMSE", f"{v1_data['train_auc']:.4f}" if v1_data['train_auc'] else "N/A",
                 f"{v2_data['train_auc']:.4f}" if v2_data['train_auc'] else "N/A"),
                ("Valid AUC/RMSE", f"{v1_data['valid_auc']:.4f}" if v1_data['valid_auc'] else "N/A",
                 f"{v2_data['valid_auc']:.4f}" if v2_data['valid_auc'] else "N/A"),
                ("Train LogLoss/MAE", f"{v1_data['train_logloss']:.4f}" if v1_data['train_logloss'] else "N/A",
                 f"{v2_data['train_logloss']:.4f}" if v2_data['train_logloss'] else "N/A"),
                ("Valid LogLoss/MAE", f"{v1_data['valid_logloss']:.4f}" if v1_data['valid_logloss'] else "N/A",
                 f"{v2_data['valid_logloss']:.4f}" if v2_data['valid_logloss'] else "N/A"),
                ("Training Duration", f"{v1_data['training_duration_seconds']:.1f}s" if v1_data['training_duration_seconds'] else "N/A",
                 f"{v2_data['training_duration_seconds']:.1f}s" if v2_data['training_duration_seconds'] else "N/A"),
                ("Status", "✅ Active" if v1_data['is_active'] else "📦 Archived",
                 "✅ Active" if v2_data['is_active'] else "📦 Archived"),
            ]

            comparison_df = pd.DataFrame(comparison_metrics, columns=[
                                         "Metric", "Version 1", "Version 2"])

            # Calculate delta where applicable
            delta_col = []
            for metric, v1, v2 in comparison_metrics:
                if metric in ["Train AUC/RMSE", "Valid AUC/RMSE", "Train LogLoss/MAE", "Valid LogLoss/MAE"]:
                    try:
                        v1_val = float(v1) if v1 != "N/A" else None
                        v2_val = float(v2) if v2 != "N/A" else None
                        if v1_val and v2_val:
                            delta = v2_val - v1_val
                            if "AUC" in metric:
                                # Higher AUC is better
                                color = "🟢" if delta > 0 else "🔴" if delta < 0 else "⚪"
                            else:
                                # Lower RMSE/LogLoss/MAE is better
                                color = "🟢" if delta < 0 else "🔴" if delta > 0 else "⚪"
                            delta_col.append(f"{color} {delta:+.4f}")
                        else:
                            delta_col.append("-")
                    except:
                        delta_col.append("-")
                else:
                    delta_col.append("-")

            comparison_df["Delta"] = delta_col

            st.dataframe(comparison_df, use_container_width=True,
                         hide_index=True)

            # Visual comparison chart
            if v1_data['model_name'] == v2_data['model_name']:
                st.markdown("#### 📊 Visual Comparison")

                metrics_to_compare = []
                if v1_data['model_name'] == 'lgbm_dropout':
                    metrics_to_compare = [
                        ('Valid AUC', v1_data['valid_auc'],
                         v2_data['valid_auc']),
                        ('Valid Accuracy',
                         v1_data['valid_accuracy'], v2_data['valid_accuracy']),
                        ('Valid LogLoss',
                         v1_data['valid_logloss'], v2_data['valid_logloss']),
                    ]
                else:
                    metrics_to_compare = [
                        ('Test RMSE', v1_data['valid_auc'],
                         v2_data['valid_auc']),
                        ('Test MAE', v1_data['valid_logloss'],
                         v2_data['valid_logloss']),
                    ]

                fig_compare = go.Figure()

                x_labels = [m[0] for m in metrics_to_compare]
                y1 = [m[1] if m[1] else 0 for m in metrics_to_compare]
                y2 = [m[2] if m[2] else 0 for m in metrics_to_compare]

                fig_compare.add_trace(go.Bar(
                    name=f'V1: {v1_data["model_version"][:8]}',
                    x=x_labels, y=y1,
                    marker_color='#667eea'
                ))
                fig_compare.add_trace(go.Bar(
                    name=f'V2: {v2_data["model_version"][:8]}',
                    x=x_labels, y=y2,
                    marker_color='#f093fb'
                ))

                fig_compare.update_layout(
                    title="Metrics Comparison",
                    barmode='group',
                    height=350
                )
                st.plotly_chart(fig_compare, use_container_width=True)

        st.markdown("---")

        # ===========================
        # MODEL DRIFT DETECTION
        # ===========================
        st.markdown("### 🔍 Model Drift Detection")

        col_drift1, col_drift2 = st.columns(2)

        with col_drift1:
            if not lgbm_data.empty and len(lgbm_data) >= 2:
                latest_lgbm = lgbm_data.iloc[-1]
                previous_lgbm = lgbm_data.iloc[-2] if len(
                    lgbm_data) >= 2 else None

                if previous_lgbm is not None and latest_lgbm['valid_auc'] and previous_lgbm['valid_auc']:
                    auc_drift = latest_lgbm['valid_auc'] - \
                        previous_lgbm['valid_auc']
                    drift_pct = (auc_drift / previous_lgbm['valid_auc']) * 100

                    if abs(drift_pct) > 5:
                        if drift_pct < 0:
                            st.error(
                                f"⚠️ **LightGBM Performance Drop Detected!**")
                            st.markdown(
                                f"AUC decreased by **{abs(drift_pct):.2f}%** since last training")
                            st.markdown(
                                "💡 Consider investigating data quality or retraining with different parameters")
                        else:
                            st.success(f"📈 **LightGBM Performance Improved!**")
                            st.markdown(
                                f"AUC increased by **{drift_pct:.2f}%** since last training")
                    else:
                        st.info(
                            f"✅ LightGBM performance stable (Drift: {drift_pct:+.2f}%)")
                else:
                    st.info("📊 Insufficient data for drift detection")
            else:
                st.info("📊 Need at least 2 training runs for drift detection")

        with col_drift2:
            if not als_data.empty and len(als_data) >= 2:
                latest_als = als_data.iloc[-1]
                previous_als = als_data.iloc[-2] if len(
                    als_data) >= 2 else None

                if previous_als is not None and latest_als['valid_auc'] and previous_als['valid_auc']:
                    rmse_drift = latest_als['valid_auc'] - \
                        previous_als['valid_auc']
                    drift_pct = (rmse_drift / previous_als['valid_auc']) * 100

                    if abs(drift_pct) > 5:
                        if drift_pct > 0:  # For RMSE, increase is bad
                            st.error(
                                f"⚠️ **Spark ALS Performance Drop Detected!**")
                            st.markdown(
                                f"RMSE increased by **{drift_pct:.2f}%** since last training")
                            st.markdown(
                                "💡 Consider investigating data quality or adjusting hyperparameters")
                        else:
                            st.success(
                                f"📈 **Spark ALS Performance Improved!**")
                            st.markdown(
                                f"RMSE decreased by **{abs(drift_pct):.2f}%** since last training")
                    else:
                        st.info(
                            f"✅ Spark ALS performance stable (Drift: {drift_pct:+.2f}%)")
                else:
                    st.info("📊 Insufficient data for drift detection")
            else:
                st.info("📊 Need at least 2 training runs for drift detection")

        st.markdown("---")

        # ===========================
        # FULL HISTORY TABLE WITH EXPORT
        # ===========================
        st.markdown("### 📋 Complete Training History")

        # Display columns selection
        display_cols = ['model_name', 'model_version', 'valid_auc', 'valid_logloss',
                        'training_samples', 'training_duration_seconds', 'is_active',
                        'created_at', 'notes']
        available_cols = [c for c in display_cols if c in full_history.columns]

        if available_cols:
            display_df = full_history[available_cols].copy()

            # Format for display
            display_df['model_version'] = display_df['model_version'].str[:15]
            if 'valid_auc' in display_df.columns:
                display_df['valid_auc'] = display_df['valid_auc'].round(4)
            if 'valid_logloss' in display_df.columns:
                display_df['valid_logloss'] = display_df['valid_logloss'].round(
                    4)
            if 'training_duration_seconds' in display_df.columns:
                display_df['training_duration_seconds'] = display_df['training_duration_seconds'].round(
                    2)
            if 'notes' in display_df.columns:
                display_df['notes'] = display_df['notes'].str[:50]

            st.dataframe(
                display_df,
                use_container_width=True,
                height=400,
                column_config={
                    "model_name": st.column_config.TextColumn("Model", width="medium"),
                    "model_version": st.column_config.TextColumn("Version", width="medium"),
                    "valid_auc": st.column_config.NumberColumn("Valid AUC/RMSE", format="%.4f"),
                    "valid_logloss": st.column_config.NumberColumn("Valid Loss/MAE", format="%.4f"),
                    "training_samples": st.column_config.NumberColumn("Samples", format="%d"),
                    "training_duration_seconds": st.column_config.NumberColumn("Duration (s)", format="%.2f"),
                    "is_active": st.column_config.CheckboxColumn("Active"),
                    "created_at": st.column_config.DatetimeColumn("Trained At", format="YYYY-MM-DD HH:mm"),
                    "notes": st.column_config.TextColumn("Notes", width="large")
                }
            )

            # Export buttons
            col_export1, col_export2, col_export3 = st.columns([1, 1, 2])

            with col_export1:
                csv_data = full_history.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Export CSV",
                    data=csv_data,
                    file_name=f"training_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )

            with col_export2:
                json_data = full_history.to_json(
                    orient='records', date_format='iso')
                st.download_button(
                    label="📥 Export JSON",
                    data=json_data,
                    file_name=f"training_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                    use_container_width=True
                )

        # ===========================
        # HYPERPARAMETER HISTORY
        # ===========================
        st.markdown("---")
        st.markdown("### ⚙️ Hyperparameter History")

        # Parse hyperparameters from JSON
        def parse_hyperparams(hp_str):
            if hp_str and isinstance(hp_str, str):
                try:
                    return json.loads(hp_str)
                except:
                    return {}
            return {}

        full_history['parsed_hyperparams'] = full_history['hyperparameters'].apply(
            parse_hyperparams)

        with st.expander("🔧 View Hyperparameters for Each Training Run", expanded=False):
            for idx, row in full_history.head(10).iterrows():
                hp = row['parsed_hyperparams']
                if hp:
                    st.markdown(
                        f"**{row['model_name']} - {row['model_version'][:15]}** ({row['created_at'].strftime('%Y-%m-%d %H:%M')})")
                    col1, col2, col3 = st.columns(3)
                    hp_items = list(hp.items())
                    for i, (key, value) in enumerate(hp_items):
                        with [col1, col2, col3][i % 3]:
                            st.code(f"{key}: {value}")
                    st.markdown("---")

    else:
        st.info("📭 No training history found for the selected filters.")
        st.markdown("""
        **Để có lịch sử training:**
        1. Chạy ETL pipeline để load dữ liệu
        2. Train models qua Airflow hoặc command line
        3. Lịch sử sẽ được tự động lưu sau mỗi lần training
        """)

# ===========================
# FOOTER
# ===========================
st.markdown("---")
st.markdown(
    """
    <div style="text-align: center; color: #666; padding: 1rem;">
        <p>📊 RecSys Monitoring Dashboard | Built with Streamlit</p>
        <p style="font-size: 0.8rem;">Last updated: {}</p>
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    unsafe_allow_html=True
)
