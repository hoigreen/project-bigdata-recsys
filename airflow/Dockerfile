FROM apache/airflow:2.8.3

USER root

# Install system dependencies for LightGBM and Java for Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libgomp1 \
        openjdk-17-jre-headless \
        procps \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Download and install Apache Spark
ENV SPARK_VERSION=3.5.3
ENV SPARK_HOME=/opt/spark
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && chmod -R 755 ${SPARK_HOME}

ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Create artifacts directory with proper permissions
RUN mkdir -p /opt/recsys-artifacts && chmod 777 /opt/recsys-artifacts

USER airflow

# Install Python packages as airflow user (including PySpark)
# Note: Install cffi first to avoid cryptography issues
RUN pip install --no-cache-dir --user \
    cffi \
    minio \
    psycopg2-binary \
    pandas \
    numpy \
    scikit-learn \
    lightgbm \
    kafka-python \
    pyspark==3.5.3

# Ensure user packages are in PATH
ENV PYTHONPATH="/home/airflow/.local/lib/python3.8/site-packages:${PYTHONPATH}"
ENV PATH="/home/airflow/.local/bin:${PATH}"
