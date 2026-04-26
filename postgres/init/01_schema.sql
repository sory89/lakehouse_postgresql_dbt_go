-- ── Lakehouse Metastore ───────────────────────────────────────────────────────

-- Pipeline run tracking
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id            SERIAL PRIMARY KEY,
    pipeline_name TEXT        NOT NULL,
    layer         TEXT        NOT NULL, -- bronze | silver | gold
    status        TEXT        NOT NULL DEFAULT 'running', -- running | success | failed
    rows_read     BIGINT      DEFAULT 0,
    rows_written  BIGINT      DEFAULT 0,
    duration_ms   INT,
    error_msg     TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);

-- Table catalog (what's in the lakehouse)
CREATE TABLE IF NOT EXISTS table_catalog (
    id            SERIAL PRIMARY KEY,
    table_name    TEXT        NOT NULL,
    layer         TEXT        NOT NULL,
    location      TEXT        NOT NULL, -- s3a://bucket/path
    format        TEXT        NOT NULL DEFAULT 'parquet',
    row_count     BIGINT      DEFAULT 0,
    size_bytes    BIGINT      DEFAULT 0,
    last_updated  TIMESTAMPTZ DEFAULT now(),
    schema_json   JSONB,
    UNIQUE (table_name, layer)
);

-- Data quality checks
CREATE TABLE IF NOT EXISTS dq_results (
    id            SERIAL PRIMARY KEY,
    table_name    TEXT        NOT NULL,
    layer         TEXT        NOT NULL,
    check_name    TEXT        NOT NULL,
    status        TEXT        NOT NULL, -- pass | fail | warn
    rows_tested   BIGINT      DEFAULT 0,
    rows_failed   BIGINT      DEFAULT 0,
    details       TEXT,
    run_at        TIMESTAMPTZ DEFAULT now()
);

-- ── dbt Gold target schemas ────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Gold: daily sales summary
CREATE TABLE IF NOT EXISTS gold.daily_sales (
    date          DATE        PRIMARY KEY,
    total_orders  INT,
    total_revenue NUMERIC(14,2),
    avg_order     NUMERIC(10,2),
    unique_customers INT,
    top_category  TEXT,
    updated_at    TIMESTAMPTZ DEFAULT now()
);

-- Gold: product performance
CREATE TABLE IF NOT EXISTS gold.product_performance (
    product_id    INT,
    product_name  TEXT,
    category      TEXT,
    period        TEXT,       -- YYYY-MM
    units_sold    INT,
    revenue       NUMERIC(14,2),
    avg_price     NUMERIC(10,2),
    return_rate   NUMERIC(5,4),
    PRIMARY KEY (product_id, period)
);

-- Gold: customer segments
CREATE TABLE IF NOT EXISTS gold.customer_segments (
    customer_id   INT         PRIMARY KEY,
    segment       TEXT,       -- vip | regular | at_risk | new
    total_spent   NUMERIC(14,2),
    order_count   INT,
    last_order_at TIMESTAMPTZ,
    clv_score     NUMERIC(16,4), -- customer lifetime value score
    updated_at    TIMESTAMPTZ DEFAULT now()
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_runs_pipeline   ON pipeline_runs(pipeline_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status     ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_dq_table        ON dq_results(table_name, run_at DESC);
