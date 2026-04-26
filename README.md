# Data Lakehouse Platform

A production-grade data lakehouse built on open-source components — from raw e-commerce events to analytical dashboards, with full Bronze → Silver → Gold medallion architecture.

---

## Architecture

```
E-commerce source data
         │
         ▼
  BRONZE LAYER  ·  MinIO  s3a://bronze
  Raw JSON/CSV, no transformation
  └── orders/year=X/month=X/day=X/*.json
  └── customers/snapshot_YYYYMMDD.csv
  └── products/snapshot_YYYYMMDD.json
         │
         │  Spark — bronze_to_silver.py
         │  Schema enforcement · deduplication
         │  Null filtering · data quality checks
         ▼
  SILVER LAYER  ·  MinIO  s3a://silver
  Cleaned Parquet, partitioned by date
  └── orders / customers / products
         │
         │  Spark — silver_to_gold.py
         │  Business aggregations
         │  Customer segmentation (RFM)
         │  Product performance
         ▼
  GOLD LAYER  ·  MinIO  s3a://gold  +  PostgreSQL
  └── daily_sales         — revenue trends
  └── product_performance — by period
  └── customer_segments   — VIP / Premium / At-risk / New
         │
         │  dbt — SQL transformations
         │  revenue_trends · top_products · customer_ltv
         ▼
  Go REST API (port 8080)  →  Streamlit Dashboard (port 8501)
```

---

## Tech stack

| Component | Technology | Role |
|---|---|---|
| Pipeline language | Python 3.11 | Ingestion, Spark jobs |
| API language | Go 1.22 | High-performance REST API |
| Batch processing | Apache Spark 3.5 | Bronze → Silver → Gold |
| SQL transformations | dbt 1.7 | Gold analytical models |
| Object storage | MinIO (S3-compatible) | All layer storage |
| Metastore + Gold DB | PostgreSQL 16 | Catalog + API backend |
| Dashboard | Streamlit 1.39 | Analytics UI |
| Infrastructure | Docker Compose | Local orchestration |

---

## Quick start

```bash
git clone https://github.com/sory89/lakehouse_postgresql_dbt_go.git
cd lakehouse_postgresql_dbt_go

# Linux / macOS
docker compose up --build

---

## Services

| Service | URL | Credentials |
|---|---|---|
| Streamlit dashboard | http://localhost:8501 | — |
| Go REST API | http://localhost:8080 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | lakehouse / lakehouse |

---

## API endpoints

```
GET  /health                          Health check
GET  /api/v1/summary                  30-day KPIs
GET  /api/v1/daily-sales?days=30      Daily revenue
GET  /api/v1/revenue-trends           7-day rolling avg + MoM growth
GET  /api/v1/top-products?limit=10    Top products by revenue
GET  /api/v1/customer-segments        Segment distribution
GET  /api/v1/customer/{id}/ltv        Customer LTV + RFM score
GET  /api/v1/pipeline-runs            Pipeline history
GET  /api/v1/catalog                  Table catalog
GET  /api/v1/dq-results               Data quality results
```

---

## Data quality checks (Silver layer)

| Table | Check | Rule |
|---|---|---|
| orders | no_null_order_id | Primary key not null |
| orders | positive_amount | total_amount > 0 |
| orders | valid_status | completed / refunded / cancelled |
| orders | valid_quantity | 1 ≤ quantity ≤ 100 |
| customers | valid_email | Contains @ |
| customers | valid_segment | standard / premium / vip |
| products | positive_price | base_price > 0 |
| products | valid_rating | 0 ≤ rating ≤ 5 |

---

## dbt models (Gold layer)

| Model | Description |
|---|---|
| `revenue_trends` | Daily revenue + 7-day rolling average + MoM growth % |
| `top_products` | Products ranked by revenue per period with share % |
| `customer_ltv` | RFM scoring → champion / loyal / new / at_risk / regular |

---

## Project structure

```
lakehouse/
├── ingestion/
│   ├── generate_data.py          E-commerce data generator → MinIO bronze
│   └── Dockerfile
├── spark/
│   ├── jobs/
│   │   ├── bronze_to_silver.py   Cleaning, DQ checks, schema enforcement
│   │   └── silver_to_gold.py     Business aggregations + PostgreSQL sync
│   └── Dockerfile
├── dbt_project/
│   ├── models/gold/
│   │   ├── revenue_trends.sql
│   │   ├── top_products.sql
│   │   ├── customer_ltv.sql
│   │   └── schema.yml            dbt tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── api/
│   ├── main.go                   Go REST API — 10 endpoints
│   ├── go.mod
│   └── Dockerfile
├── streamlit/
│   └── app.py                    Analytics dashboard — 4 tabs
├── postgres/init/
│   └── 01_schema.sql             Metastore + Gold schemas
└── docker-compose.yml
```

---
