# 📚 MOOC Recommendation System

A **Big Data Recommendation System** for Massive Open Online Courses (MOOC) built with modern data engineering practices. This project demonstrates an end-to-end ML pipeline featuring real-time event streaming, batch model training, and interactive monitoring.

![Architecture](https://img.shields.io/badge/Architecture-Microservices-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MOOC Recommendation System                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐     ┌──────────┐     ┌──────────────────────────────────────┐ │
│  │  MinIO   │────▶│   ETL    │────▶│           PostgreSQL                 │ │
│  │(S3 Store)│     │ Scripts  │     │  • users • interactions • als_*     │ │
│  └──────────┘     └──────────┘     └──────────────────────────────────────┘ │
│       │                                         │                            │
│       │                                         ▼                            │
│       │           ┌─────────────────────────────────────────────────────┐   │
│       │           │              Training Pipeline                       │   │
│       │           │  ┌─────────────┐    ┌──────────────────────────┐   │   │
│       │           │  │  LightGBM   │    │     Spark ALS Cluster    │   │   │
│       │           │  │  (Dropout)  │    │  (Collaborative Filter)  │   │   │
│       │           │  └─────────────┘    └──────────────────────────┘   │   │
│       │           └─────────────────────────────────────────────────────┘   │
│       │                                         │                            │
│       │                                         ▼                            │
│       │           ┌─────────────────────────────────────────────────────┐   │
│       │           │            Real-time Streaming Layer                 │   │
│       │           │  ┌────────────┐    ┌─────────┐    ┌─────────────┐  │   │
│       │           │  │  Producer  │───▶│  Kafka  │───▶│  Consumer   │  │   │
│       │           │  │ (Simulate) │    │ (Queue) │    │ (Recommend) │  │   │
│       │           │  └────────────┘    └─────────┘    └─────────────┘  │   │
│       │           └─────────────────────────────────────────────────────┘   │
│       │                                                                      │
│       ▼                                                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                      Orchestration & Monitoring                        │  │
│  │  ┌──────────────────────┐         ┌────────────────────────────────┐ │  │
│  │  │   Apache Airflow     │         │   Streamlit Dashboard          │ │  │
│  │  │   (DAG Scheduler)    │         │   (Real-time Monitoring)       │ │  │
│  │  └──────────────────────┘         └────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

### 🎯 Core Capabilities

- **Dropout Prediction**: LightGBM model predicts course completion probability based on user behavior
- **Collaborative Filtering**: Spark ALS for user-item recommendations
- **Real-time Streaming**: Kafka-based event processing with live recommendations
- **Hot Model Reload**: Consumer automatically detects and loads updated models

### 📊 Monitoring Dashboard

- **Service Health Monitoring**: Real-time status of all infrastructure components
- **Live Event Streaming**: Watch Kafka events flow in real-time
- **Analytics & Insights**: Pass/fail distributions, user activity, course statistics
- **Auto-refresh**: Configurable refresh rates for live data

### 🔄 Data Pipeline

- **ETL from MinIO**: Load CSV data from S3-compatible object storage
- **Airflow Orchestration**: Scheduled and manual pipeline triggers
- **Incremental Updates**: Support for continuous data ingestion

### 🔁 Periodic Model Retraining (NEW)

- **Scheduled Retraining**: Daily automatic model updates with fresh data from DB
- **Model Versioning**: Timestamp-based versions with configurable retention
- **Training History**: All metrics logged to PostgreSQL for tracking
- **On-Demand Retraining**: Manual trigger with configurable hyperparameters
- **Zero Downtime**: Consumer hot-reloads new model automatically

---

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|------------|---------|
| **Orchestration** | Apache Airflow | 2.8.3 |
| **Object Storage** | MinIO | Latest |
| **Database** | PostgreSQL | 15 |
| **Message Queue** | Apache Kafka | 7.6.0 |
| **Distributed Computing** | Apache Spark | 3.5.3 |
| **ML Framework** | LightGBM | Latest |
| **Dashboard** | Streamlit | 1.40.2 |
| **Containerization** | Docker Compose | 3.x |

---

## 📁 Project Structure

```
bigdata-recsys/
├── 📂 airflow/                    # Airflow configuration
│   ├── dags/
│   │   └── recsys_pipeline.py     # Main DAG definitions
│   ├── Dockerfile                 # Custom Airflow image
│   └── requirements.txt           # Airflow dependencies
│
├── 📂 core-logic/                 # ML and streaming core
│   ├── train_module.py            # LightGBM training script
│   ├── run_producer.py            # Kafka event producer
│   └── run_consumer.py            # Real-time recommendation consumer
│
├── 📂 dashboard/                  # Monitoring dashboard
│   ├── app.py                     # Streamlit application
│   ├── Dockerfile                 # Dashboard container
│   └── requirements.txt           # Dashboard dependencies
│
├── 📂 etl/                        # Data loading scripts
│   ├── load_users_from_minio.py   # User data ETL
│   └── load_interactions_from_minio.py  # Interactions ETL
│
├── 📂 spark_jobs/                 # Spark ML jobs
│   └── batch_als_train.py         # ALS collaborative filtering
│
├── 📂 sql/                        # Database schemas
│   └── init.sql                   # Table definitions
│
├── 📂 data/                       # Data directory (gitignored)
│   ├── minio/                     # MinIO storage
│   └── pgdata/                    # PostgreSQL data
│
├── docker-compose.yml             # Service orchestration
├── .gitignore                     # Git ignore rules
└── README.md                      # This file
```

---

## 🚀 Quick Start

### Prerequisites

- **Docker** >= 20.10
- **Docker Compose** >= 2.0
- **RAM** >= 8GB recommended
- **Disk** >= 10GB free space

### 1. Clone & Start Services

```bash
# Clone the repository
git clone <repository-url>
cd bigdata-recsys

# Start all services
docker-compose up -d
```

### 2. Verify Services are Running

```bash
# Check container status
docker-compose ps

# Expected output: All services should be "Up"
```

### 3. Upload Data to MinIO

1. Open MinIO Console: <http://localhost:9001>
2. Login: `minioadmin` / `minioadmin`
3. Create bucket: `mooc-data`
4. Upload your CSV files:
   - `user_info.csv` - User demographics
   - `train.csv` - User-course interactions

### 4. Run the Pipeline

1. Open Airflow: <http://localhost:8080>
2. Login: `admin` / `admin`
3. Enable and trigger `recsys_pipeline` DAG

### 5. Monitor in Dashboard

Open Streamlit Dashboard: <http://localhost:8501>

---

## 🌐 Service URLs & Ports

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | <http://localhost:8080> | admin / admin |
| **Streamlit Dashboard** | <http://localhost:8501> | - |
| **MinIO Console** | <http://localhost:9001> | minioadmin / minioadmin |
| **Spark Master UI** | <http://localhost:8081> | - |
| **Spark Worker UI** | <http://localhost:8082> | - |
| **PostgreSQL** | localhost:5433 | postgres / postgres |
| **Kafka** | localhost:9092 | - |
| **MinIO API** | localhost:9000 | - |

---

## 📋 Airflow DAGs

### `recsys_pipeline` (Main Pipeline)

Complete pipeline: ETL → Train → Stream

```
load_users → load_interactions → [train_lgbm, spark_als_train] → training_complete → [run_producer, run_consumer]
```

### `recsys_periodic_retrain` (Scheduled Retraining)

Automatic periodic retraining of LightGBM model with fresh data from DB

```
check_data_freshness → retrain_lgbm → retrain_complete
```

**Schedule**: Daily at 2:00 AM (cron: `0 2 * * *`)

### `recsys_retrain_on_demand` (Manual Retraining)

On-demand retraining with configurable hyperparameters

**Parameters**:

- `learning_rate`: 0.05 (default)
- `num_leaves`: 31 (default)
- `num_boost_round`: 500 (default)
- `early_stopping`: 50 (default)
- `notes`: Custom notes for training run

---

## 📊 Data Schema

### `users` Table

| Column | Type | Description |
|--------|------|-------------|
| user_id | BIGINT | Primary key |
| gender | VARCHAR(10) | User gender |
| education | VARCHAR(50) | Education level |
| birth_year | INT | Year of birth |

### `interactions` Table

| Column | Type | Description |
|--------|------|-------------|
| user_id | BIGINT | Foreign key to users |
| course_id | TEXT | Course identifier |
| truth | SMALLINT | 0=Pass, 1=Fail |
| action_* | DOUBLE | Behavioral features (22 columns) |
| event_ts | BIGINT | Event timestamp |

### `als_user_factors` / `als_item_factors`

Latent factors from Spark ALS training stored as JSON arrays.

### `model_training_history` (NEW)

| Column | Type | Description |
|--------|------|-------------|
| model_version | VARCHAR | Timestamp-based version (YYYYMMDD_HHMMSS) |
| train_auc / valid_auc | DOUBLE | Training/validation AUC scores |
| training_samples | INT | Number of training samples |
| hyperparameters | TEXT | JSON of training hyperparameters |
| training_duration_seconds | DOUBLE | Time taken to train |
| is_active | BOOLEAN | Currently active model flag |

---

## 🎯 ML Models

### LightGBM Dropout Predictor

- **Task**: Binary classification (Pass/Fail)
- **Features**: 24 behavioral features per user-course pair
- **Output**: Probability of course dropout

### Spark ALS Collaborative Filter

- **Task**: Matrix factorization for recommendations
- **Rank**: 50 latent factors
- **Output**: User and item embeddings in PostgreSQL

---

## 🖥️ Dashboard Features

### Overview Tab

- Service health status (PostgreSQL, Kafka, MinIO, Spark, Airflow)
- Database statistics (users, interactions, courses)
- ALS model status
- Kafka streaming metrics

### Analytics Tab

- Pass/Fail distribution charts
- User action distribution
- Education and gender demographics
- Course enrollment histograms

### Real-time Events Tab

- Live Kafka event stream
- Active users and courses
- Events per minute rate
- Recent events table

### Course Stats Tab

- Pass rate by course
- Enrollment rankings
- Detailed course metrics table

### User Stats Tab

- Top active users
- User performance scatter plots
- Power user identification

---

## 🔧 Development

### Running Locally (Outside Docker)

```bash
# Install dependencies
pip install -r airflow/requirements.txt
pip install -r dashboard/requirements.txt

# Set environment variables for local execution
export PG_HOST=localhost
export PG_PORT=5433
export KAFKA_BOOTSTRAP=localhost:9092

# Run training
python core-logic/train_module.py

# Run streaming (separate terminals)
python core-logic/run_producer.py
python core-logic/run_consumer.py

# Run dashboard
streamlit run dashboard/app.py
```

### Rebuilding Containers

```bash
# Rebuild specific service
docker-compose build airflow
docker-compose up -d airflow

# Rebuild all services
docker-compose build
docker-compose up -d
```

### Viewing Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow
docker-compose logs -f dashboard
```

---

## 📝 Data Format

### user_info.csv

```csv
user_id,gender,education,birth
1001,M,Bachelor,1995
1002,F,Master,1992
```

### train.csv

```csv
username,course_id,truth,action_play_video,action_pause_video,...,timestamp
1001,course-v1:edX+DemoX+Demo,0,15,3,...,1609459200
1002,course-v1:edX+DemoX+Demo,1,2,0,...,1609459300
```

**Note**: `truth = 0` means Pass, `truth = 1` means Fail

---

## 🐛 Troubleshooting

### Services won't start

```bash
# Check Docker resources
docker system df

# Clean up unused resources
docker system prune -a

# Restart with fresh volumes
docker-compose down -v
docker-compose up -d
```

### Kafka connection issues

```bash
# Verify Kafka is running
docker-compose logs kafka

# Check topic exists
docker exec recsys-kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Database connection refused

```bash
# Check PostgreSQL status
docker-compose logs postgres

# Verify init.sql was executed
docker exec -it recsys-postgres psql -U postgres -d recsys -c "\dt"
```

### Airflow DAG not visible

```bash
# Restart Airflow to pick up DAG changes
docker-compose restart airflow

# Check DAG parsing errors
docker exec recsys-airflow airflow dags list
```

---

## 🔐 Security Notes

⚠️ **For Development Only** - Default credentials are used for simplicity:

- PostgreSQL: `postgres` / `postgres`
- MinIO: `minioadmin` / `minioadmin`
- Airflow: `admin` / `admin`

For production, please:

1. Change all default passwords
2. Use secrets management (Docker secrets, Vault, etc.)
3. Enable TLS/SSL for all services
4. Configure proper network isolation

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📧 Contact

For questions or feedback, please open an issue in this repository.

---

<p align="center">
  Built with ❤️ for Big Data and Machine Learning
</p>
