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
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏠 Overview",
    "📈 Analytics",
    "🔄 Real-time Events",
    "📚 Course Stats",
    "👥 User Stats"
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
