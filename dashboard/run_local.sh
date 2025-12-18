#!/bin/bash
# Run Streamlit Dashboard Locally (for development)

# Set environment variables for local development
export PG_HOST=localhost
export PG_PORT=5433
export PG_USER=postgres
export PG_PASSWORD=postgres
export PG_DB=recsys

export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_TOPIC=mooc-events

export MINIO_ENDPOINT=localhost:9000
export SPARK_MASTER_UI=http://localhost:8081
export AIRFLOW_UI=http://localhost:8080

export ARTIFACT_DIR=/tmp/recsys-artifacts

# Install dependencies if needed
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
pip install -r requirements.txt -q

# Run Streamlit
echo "Starting Streamlit Dashboard..."
echo "Access at: http://localhost:8501"
streamlit run app.py --server.port=8501
