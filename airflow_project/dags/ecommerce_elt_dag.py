"""
Daily ELT pipeline DAG.
Schedule: 05:00 UTC daily.
Flow: S3 sensors → parallel Snowflake loads → row count validation
      → dbt staging run/test → dbt marts run/test → notify
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator   # ← fixed
from airflow.providers.standard.operators.bash import BashOperator       # ← fixed
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.task.trigger_rule import TriggerRule                        # ← fixed

default_args = {
    "owner":                     "data-engineering",
    "depends_on_past":           False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    # email_on_failure/retry removed — use SmtpNotifier on_failure_callback instead
}

S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "surya-ecommerce-raw-data")
DBT_DIR   = "/usr/local/airflow/dags/dbt_project"

DBT_ENV = {
    "SNOWFLAKE_ACCOUNT":  os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "SNOWFLAKE_USER":     os.environ.get("SNOWFLAKE_USER", ""),
    "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD", ""),
}

with DAG(
    dag_id              = "ecommerce_elt_daily",
    description         = "Daily ELT: S3 → Snowflake → dbt staging → dbt marts",
    default_args        = default_args,
    start_date          = datetime(2024, 1, 1),
    schedule            = "@daily",
    catchup             = False,
    max_active_runs     = 1,
    template_searchpath = ["/usr/local/airflow/include"],  # ← fixes TemplateNotFound
    tags                = ["ecommerce", "elt", "dbt", "snowflake"],
) as dag:

    # ── Wait for S3 files (runs in parallel) ──────────────────────────────────
    wait_orders = S3KeySensor(
        task_id       = "wait_for_orders_s3",
        bucket_name   = S3_BUCKET,
        bucket_key    = "raw/orders/date={{ ds }}/orders_{{ ds }}.csv",
        aws_conn_id   = "aws_default",
        timeout       = 7200,
        poke_interval = 60,
        mode          = "reschedule",
    )

    wait_clicks = S3KeySensor(
        task_id       = "wait_for_clicks_s3",
        bucket_name   = S3_BUCKET,
        bucket_key    = "raw/clicks/date={{ ds }}/clicks_{{ ds }}.csv",
        aws_conn_id   = "aws_default",
        timeout       = 7200,
        poke_interval = 60,
        mode          = "reschedule",
    )

    wait_sessions = S3KeySensor(
        task_id       = "wait_for_sessions_s3",
        bucket_name   = S3_BUCKET,
        bucket_key    = "raw/sessions/date={{ ds }}/sessions_{{ ds }}.csv",
        aws_conn_id   = "aws_default",
        timeout       = 7200,
        poke_interval = 60,
        mode          = "reschedule",
    )

    # ── Load to Snowflake (runs in parallel after all sensors pass) ───────────
    load_orders = SQLExecuteQueryOperator(
        task_id    = "load_orders_to_snowflake",
        conn_id    = "snowflake_default",
        sql        = "sql/load_orders.sql",    # ← relative to template_searchpath
    )

    load_clicks = SQLExecuteQueryOperator(
        task_id    = "load_clicks_to_snowflake",
        conn_id    = "snowflake_default",
        sql        = "sql/load_clicks.sql",    # ← relative to template_searchpath
    )

    load_sessions = SQLExecuteQueryOperator(
        task_id    = "load_sessions_to_snowflake",
        conn_id    = "snowflake_default",
        sql        = "sql/load_sessions.sql",  # ← relative to template_searchpath
    )

    # ── Validate row counts ───────────────────────────────────────────────────
    validate_counts = SQLExecuteQueryOperator(
        task_id = "validate_row_counts",
        conn_id = "snowflake_default",
        sql     = """
            SELECT
                CASE WHEN (SELECT COUNT(*) FROM ECOMMERCE_DB.RAW.ORDERS_RAW
                           WHERE DATE(_loaded_at) = '{{ ds }}') = 0
                     THEN 1/0 ELSE 1 END AS orders_check,
                CASE WHEN (SELECT COUNT(*) FROM ECOMMERCE_DB.RAW.CLICKS_RAW
                           WHERE DATE(_loaded_at) = '{{ ds }}') = 0
                     THEN 1/0 ELSE 1 END AS clicks_check,
                CASE WHEN (SELECT COUNT(*) FROM ECOMMERCE_DB.RAW.SESSIONS_RAW
                           WHERE DATE(_loaded_at) = '{{ ds }}') = 0
                     THEN 1/0 ELSE 1 END AS sessions_check
        """,
    )

    # ── dbt run + test ────────────────────────────────────────────────────────
    dbt_run_staging = BashOperator(
        task_id      = "dbt_run_staging",
        bash_command = f"cd {DBT_DIR} && dbt run --select staging --profiles-dir . --target prod",
        env          = DBT_ENV,
    )

    dbt_test_staging = BashOperator(
        task_id      = "dbt_test_staging",
        bash_command = f"cd {DBT_DIR} && dbt test --select staging --profiles-dir . --target prod",
        env          = DBT_ENV,
    )

    dbt_run_marts = BashOperator(
        task_id      = "dbt_run_marts",
        bash_command = f"cd {DBT_DIR} && dbt run --select marts --profiles-dir . --target prod",
        env          = DBT_ENV,
    )

    dbt_test_marts = BashOperator(
        task_id      = "dbt_test_marts",
        bash_command = f"cd {DBT_DIR} && dbt test --select marts --profiles-dir . --target prod",
        env          = DBT_ENV,
    )

    # ── Notifications ─────────────────────────────────────────────────────────
    notify_success = BashOperator(
        task_id      = "notify_success",
        bash_command = "echo '✅ ELT pipeline completed for {{ ds }}'",
    )

    notify_failure = BashOperator(
        task_id      = "notify_failure",
        bash_command = "echo '❌ ELT pipeline FAILED for {{ ds }}'",
        trigger_rule = TriggerRule.ONE_FAILED,
    )

    # ── DAG wiring ────────────────────────────────────────────────────────────
    [wait_orders, wait_clicks, wait_sessions] >> load_orders
    [wait_orders, wait_clicks, wait_sessions] >> load_clicks
    [wait_orders, wait_clicks, wait_sessions] >> load_sessions

    [load_orders, load_clicks, load_sessions] >> validate_counts

    validate_counts >> dbt_run_staging >> dbt_test_staging
    dbt_test_staging >> dbt_run_marts >> dbt_test_marts
    dbt_test_marts >> notify_success

    [
        wait_orders, wait_clicks, wait_sessions,
        load_orders, load_clicks, load_sessions,
        validate_counts, dbt_run_staging, dbt_test_staging,
        dbt_run_marts, dbt_test_marts,
    ] >> notify_failure