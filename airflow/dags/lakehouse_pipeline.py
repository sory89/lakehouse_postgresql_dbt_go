"""
Lakehouse Pipeline DAG
Orchestrates: Bronze → Silver → Gold → dbt → tests
Schedule: every hour
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_PKGS   = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="lakehouse_pipeline",
    description="Bronze → Silver → Gold → dbt lakehouse pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["lakehouse", "etl", "spark", "dbt"],
) as dag:

    # ── Bronze → Silver ───────────────────────────────────────────────────────
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"""
        {SPARK_SUBMIT} \
          --master local[2] \
          --packages {SPARK_PKGS} \
          /opt/spark-jobs/bronze_to_silver.py
        """,
    )

    # ── Silver → Gold ─────────────────────────────────────────────────────────
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"""
        {SPARK_SUBMIT} \
          --master local[2] \
          --packages {SPARK_PKGS} \
          /opt/spark-jobs/silver_to_gold.py
        """,
    )

    # ── dbt run ───────────────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /dbt && dbt run \
          --profiles-dir . \
          --project-dir . \
          --select gold
        """,
    )

    # ── dbt test ──────────────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /dbt && dbt test \
          --profiles-dir . \
          --project-dir . \
          --select gold
        """,
    )

    # ── Pipeline ──────────────────────────────────────────────────────────────
    bronze_to_silver >> silver_to_gold >> dbt_run >> dbt_test
