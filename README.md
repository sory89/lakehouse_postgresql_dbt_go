# 🏔️ Data Lakehouse Platform (Production-grade)

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-blue?logo=python">
  <img src="https://img.shields.io/badge/Go-1.22-cyan?logo=go">
  <img src="https://img.shields.io/badge/Apache%20Spark-3.5-orange?logo=apachespark">
  <img src="https://img.shields.io/badge/dbt-1.7-red?logo=dbt">
  <img src="https://img.shields.io/badge/MinIO-S3--compatible-purple">
  <img src="https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql">
  <img src="https://img.shields.io/badge/Streamlit-1.39-red?logo=streamlit">
  <img src="https://img.shields.io/badge/Docker-Compose-blue?logo=docker">
</p>

---

## 🏗️ Architecture

```
E-commerce Source Data
        │
        ▼
   🥉 BRONZE LAYER (MinIO s3a://bronze)
   Raw JSON/CSV files — no transformation
   ├── orders/year=X/month=X/day=X/*.json
   ├── customers/snapshot_YYYYMMDD.csv
   └── products/snapshot_YYYYMMDD.json
        │
        ▼ Spark (bronze_to_silver.py)
        │ • Schema enforcement
        │ • Deduplication
        │ • Null filtering
        │ • Data quality checks
        │
   🥈 SILVER LAYER (MinIO s3a://silver)
   Cleaned Parquet, partitioned
   ├── orders/     (partitioned by year/month/day)
   ├── customers/
   └── products/
        │
        ▼ Spark (silver_to_gold.py)
        │ • Business aggregations
        │ • Customer segmentation (RFM)
        │ • Product performance
        │
   🥇 GOLD LAYER (MinIO s3a://gold + PostgreSQL)
   ├── daily_sales         (revenue trends)
   ├── product_performance (by period)
   └── customer_segments   (VIP/Premium/At-risk/New)
        │
        ▼ dbt (SQL transformations)
        │ • revenue_trends   (7d rolling avg, MoM growth)
        │ • top_products     (ranked by revenue)
        │ • customer_ltv     (RFM scoring)
        │
   Go API (8080) → Streamlit Dashboard (8501)
```

---

## ⚙️ Tech Stack

| Component | Technology | Role |
|-----------|------------|------|
| Language (pipelines) | Python 3.11 | Ingestion, Spark jobs |
| Language (API) | Go 1.22 | High-performance REST API |
| Batch processing | Apache Spark 3.5 | Bronze→Silver→Gold |
| SQL transformations | dbt 1.7 | Gold analytical models |
| Object storage | MinIO (S3-compatible) | All layer storage |
| Metastore + Gold | PostgreSQL 16 | Catalog + API backend |
| Dashboard | Streamlit | Analytics UI |

---

## 🚀 Quick Start

```bash
git clone <repo>
cd lakehouse

# Windows
$env:DOCKER_BUILDKIT=0
docker compose up --build

# Linux/macOS
docker compose up --build
```

### Run the pipeline manually

```bash
# 1. Wait for generator to write some data (1-2 min)
docker logs lakehouse-generator

# 2. Run Bronze → Silver
docker compose run --rm spark-bronze-silver

# 3. Run Silver → Gold
docker compose run --rm spark-silver-gold

# 4. Run dbt models
docker compose run --rm dbt
```

---

## 🌐 Services

| Service | URL | Description |
|---------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | Analytics UI |
| Go API | http://localhost:8080 | Gold layer REST API |
| MinIO Console | http://localhost:9001 | Object storage UI (minioadmin/minioadmin) |
| PostgreSQL | localhost:5432 | Metastore (lakehouse/lakehouse) |

---

## 📡 Go API Endpoints

```bash
GET /health                         # Health check
GET /api/v1/summary                 # 30-day KPIs
GET /api/v1/daily-sales?days=30     # Daily revenue
GET /api/v1/revenue-trends          # 7d rolling avg + MoM growth
GET /api/v1/top-products?limit=10   # Top products by revenue
GET /api/v1/customer-segments       # Segment distribution
GET /api/v1/customer/{id}/ltv       # Customer LTV + RFM
GET /api/v1/pipeline-runs           # Pipeline history
GET /api/v1/catalog                 # Table catalog
GET /api/v1/dq-results              # Data quality results
```

---

## 🔍 Data Quality Checks (Silver layer)

| Table | Check | Description |
|-------|-------|-------------|
| orders | `no_null_order_id` | Primary key not null |
| orders | `positive_amount` | total_amount > 0 |
| orders | `valid_status` | completed/refunded/cancelled |
| orders | `valid_quantity` | 1 ≤ quantity ≤ 100 |
| customers | `valid_email` | Contains @ |
| customers | `valid_segment` | standard/premium/vip |
| products | `positive_price` | base_price > 0 |
| products | `valid_rating` | 0 ≤ rating ≤ 5 |

---

## 📊 dbt Models (Gold)

| Model | Description |
|-------|-------------|
| `revenue_trends` | Daily revenue + 7-day rolling avg + MoM growth % |
| `top_products` | Products ranked by revenue per period + share % |
| `customer_ltv` | RFM scoring → champion/loyal/new/at_risk/regular |

---

## 📁 Project Structure

```
lakehouse/
├── ingestion/
│   ├── generate_data.py    # E-commerce data generator → MinIO bronze
│   └── Dockerfile
├── spark/
│   ├── jobs/
│   │   ├── bronze_to_silver.py   # Cleaning + DQ + schema enforcement
│   │   └── silver_to_gold.py     # Business aggregations + PostgreSQL sync
│   └── Dockerfile
├── dbt_project/
│   ├── models/gold/
│   │   ├── revenue_trends.sql
│   │   ├── top_products.sql
│   │   ├── customer_ltv.sql
│   │   └── schema.yml           # dbt tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── api/
│   ├── main.go                  # Go REST API (10 endpoints)
│   ├── go.mod
│   └── Dockerfile
├── streamlit/
│   └── app.py                   # Dark analytics dashboard (4 tabs)
├── postgres/init/
│   └── 01_schema.sql            # Metastore + Gold schemas
└── docker-compose.yml
```

---

## 📝 License

MIT
# lakehouse_postgresql_dbt_go
