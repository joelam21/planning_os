from __future__ import annotations

from datetime import datetime, timedelta
import os
import pendulum
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DEFAULT_SOURCE = "iowa_liquor"
ALLOWED_SOURCES = {"iowa_liquor"}
DEFAULT_BATCH_SIZE = 1000
DEFAULT_MAX_BATCHES = 2000
DEFAULT_MAX_MANUAL_WINDOW_DAYS = 366
DEFAULT_MAX_INGESTION_LAG_HOURS = 24


def _get_positive_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        parsed_value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer, got {raw_value!r}") from exc

    if parsed_value <= 0:
        raise ValueError(f"Environment variable {name} must be > 0, got {parsed_value}")

    return parsed_value


def _get_positive_int_conf(conf: dict[str, object], key: str, default: int) -> int:
    raw_value = conf.get(key)
    if raw_value in (None, ""):
        return default

    try:
        parsed_value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"dag_run.conf[{key!r}] must be an integer, got {raw_value!r}") from exc

    if parsed_value <= 0:
        raise ValueError(f"dag_run.conf[{key!r}] must be > 0, got {parsed_value}")

    return parsed_value


def _parse_iso_date(value: Any, field_name: str) -> pendulum.Date:
    if value is None:
        raise ValueError(f"{field_name} is required")

    try:
        return pendulum.from_format(str(value), "YYYY-MM-DD", tz="UTC").date()
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"{field_name} must be YYYY-MM-DD, got {value!r}") from exc


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable is missing: {name}")
    return value


def _get_snowflake_connection_params() -> dict[str, Any]:
    params: dict[str, Any] = {
        "account": _require_env("DBT_ACCOUNT"),
        "user": _require_env("DBT_USER"),
        "password": _require_env("DBT_PASSWORD"),
        "role": _require_env("DBT_ROLE"),
        "warehouse": _require_env("DBT_WAREHOUSE"),
        "database": _require_env("DBT_DATABASE"),
        "schema": _require_env("DBT_SCHEMA"),
    }

    authenticator = os.getenv("DBT_AUTHENTICATOR")
    if authenticator:
        params["authenticator"] = authenticator

    return params


def validate_run_contract(**context) -> None:
    ti = context["ti"]
    source = ti.xcom_pull(task_ids="compute_run_window", key="source")
    start_date_raw = ti.xcom_pull(task_ids="compute_run_window", key="start_date")
    end_date_raw = ti.xcom_pull(task_ids="compute_run_window", key="end_date")
    batch_size = ti.xcom_pull(task_ids="compute_run_window", key="batch_size")
    max_batches = ti.xcom_pull(task_ids="compute_run_window", key="max_batches")
    window_mode = ti.xcom_pull(task_ids="compute_run_window", key="window_mode")

    if source not in ALLOWED_SOURCES:
        raise ValueError(
            f"source must be one of {sorted(ALLOWED_SOURCES)}, got {source!r}"
        )

    start_date = _parse_iso_date(start_date_raw, "start_date")
    end_date = _parse_iso_date(end_date_raw, "end_date")

    if end_date < start_date:
        raise ValueError(
            f"end_date must be >= start_date, got {start_date} to {end_date}"
        )

    window_days = (end_date - start_date).days + 1
    if window_days <= 0:
        raise ValueError(f"window_days must be > 0, got {window_days}")

    max_manual_window_days = _get_positive_int_env(
        "PLANNING_OS_MAX_MANUAL_WINDOW_DAYS",
        DEFAULT_MAX_MANUAL_WINDOW_DAYS,
    )
    if window_mode == "manual" and window_days > max_manual_window_days:
        raise ValueError(
            "manual window exceeds configured maximum: "
            f"window_days={window_days}, max_allowed={max_manual_window_days}"
        )

    if int(batch_size) <= 0:
        raise ValueError(f"batch_size must be > 0, got {batch_size}")
    if int(max_batches) <= 0:
        raise ValueError(f"max_batches must be > 0, got {max_batches}")

    ti.xcom_push(key="window_days", value=window_days)
    print(
        "Run contract validated: "
        f"mode={window_mode}, source={source}, start={start_date}, end={end_date}, "
        f"window_days={window_days}, batch_size={batch_size}, max_batches={max_batches}"
    )


def validate_data_contract(**context) -> None:
    import snowflake.connector

    ti = context["ti"]
    source = ti.xcom_pull(task_ids="compute_run_window", key="source")
    start_date = ti.xcom_pull(task_ids="compute_run_window", key="start_date")
    end_date = ti.xcom_pull(task_ids="compute_run_window", key="end_date")

    if source != "iowa_liquor":
        raise ValueError(f"Unsupported source for data contract validation: {source!r}")

    params = _get_snowflake_connection_params()
    schema = params["schema"]
    database = params["database"]

    max_ingestion_lag_hours = _get_positive_int_env(
        "PLANNING_OS_MAX_INGESTION_LAG_HOURS",
        DEFAULT_MAX_INGESTION_LAG_HOURS,
    )

    sql = f"""
        select
            count(*) as row_count,
            min(order_date) as min_order_date,
            max(order_date) as max_order_date,
            max(loaded_at) as max_loaded_at,
            datediff('hour', max(loaded_at), current_timestamp()) as loaded_at_lag_hours
        from {database}.{schema}.RAW_IOWA_LIQUOR
        where order_date between to_date(%s) and to_date(%s)
    """

    with snowflake.connector.connect(**params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (start_date, end_date))
            row = cursor.fetchone()

    if row is None:
        raise ValueError("Data contract validation returned no result row")

    row_count, min_order_date, max_order_date, max_loaded_at, loaded_at_lag_hours = row

    if row_count <= 0:
        raise ValueError(
            "No rows landed for requested window: "
            f"source={source}, start_date={start_date}, end_date={end_date}"
        )

    if min_order_date is None or max_order_date is None:
        raise ValueError("order_date bounds are null for landed data")

    requested_start = _parse_iso_date(start_date, "start_date")
    requested_end = _parse_iso_date(end_date, "end_date")

    if max_order_date < requested_start or min_order_date > requested_end:
        raise ValueError(
            "Landed data does not overlap requested window: "
            f"requested=[{requested_start}, {requested_end}] "
            f"landed=[{min_order_date}, {max_order_date}]"
        )

    if max_loaded_at is None:
        raise ValueError("loaded_at is null for landed data")

    if loaded_at_lag_hours is None:
        raise ValueError("loaded_at_lag_hours is null")

    if loaded_at_lag_hours > max_ingestion_lag_hours:
        raise ValueError(
            "Landed data is stale beyond configured threshold: "
            f"loaded_at_lag_hours={loaded_at_lag_hours}, "
            f"max_allowed={max_ingestion_lag_hours}"
        )

    ti.xcom_push(key="ingested_row_count", value=int(row_count))
    ti.xcom_push(key="ingested_min_order_date", value=str(min_order_date))
    ti.xcom_push(key="ingested_max_order_date", value=str(max_order_date))
    ti.xcom_push(key="ingested_max_loaded_at", value=str(max_loaded_at))
    ti.xcom_push(key="ingested_loaded_at_lag_hours", value=int(loaded_at_lag_hours))

    print(
        "Data contract validated: "
        f"row_count={row_count}, min_order_date={min_order_date}, "
        f"max_order_date={max_order_date}, max_loaded_at={max_loaded_at}, "
        f"loaded_at_lag_hours={loaded_at_lag_hours}"
    )


def check_pipeline_health(**context) -> None:
    import snowflake.connector

    params = _get_snowflake_connection_params()
    schema = params["schema"]
    database = params["database"]

    sql = f"""
        select
            freshness_status,
            snapshot_status,
            raw_lag_days,
            snapshot_lag_days,
            fact_row_count,
            daily_store_day_count,
            sku_velocity_row_count,
            replenishment_forecast_row_count
        from {database}.{schema}.MON_PIPELINE_HEALTH
        order by observed_at desc
        limit 1
    """

    with snowflake.connector.connect(**params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()

    if row is None:
        raise ValueError("MON_PIPELINE_HEALTH returned no rows")

    (
        freshness_status,
        snapshot_status,
        raw_lag_days,
        snapshot_lag_days,
        fact_row_count,
        daily_store_day_count,
        sku_velocity_row_count,
        replenishment_forecast_row_count,
    ) = row

    status_values = {str(freshness_status).upper(), str(snapshot_status).upper()}
    if "ERROR" in status_values:
        raise ValueError(
            "Pipeline health check failed: "
            f"freshness_status={freshness_status}, snapshot_status={snapshot_status}, "
            f"raw_lag_days={raw_lag_days}, snapshot_lag_days={snapshot_lag_days}"
        )

    context["ti"].xcom_push(key="freshness_status", value=str(freshness_status))
    context["ti"].xcom_push(key="snapshot_status", value=str(snapshot_status))

    print(
        "Pipeline health check passed: "
        f"freshness_status={freshness_status}, snapshot_status={snapshot_status}, "
        f"raw_lag_days={raw_lag_days}, snapshot_lag_days={snapshot_lag_days}, "
        f"fact_row_count={fact_row_count}, daily_store_day_count={daily_store_day_count}, "
        f"sku_velocity_row_count={sku_velocity_row_count}, "
        f"replenishment_forecast_row_count={replenishment_forecast_row_count}"
    )


def compute_run_window(**context) -> None:
    """
    Compute a default rolling ingestion window.

    Default behavior:
    - If start_date and end_date are provided in dag_run.conf, use them.
    - Otherwise use a rolling 90-day window ending yesterday in UTC.

    Stores values in XCom for downstream tasks.
    """
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    default_batch_size = _get_positive_int_env("PLANNING_OS_BATCH_SIZE", DEFAULT_BATCH_SIZE)
    default_max_batches = _get_positive_int_env("PLANNING_OS_MAX_BATCHES", DEFAULT_MAX_BATCHES)

    if conf.get("start_date") and conf.get("end_date"):
        start_date = conf["start_date"]
        end_date = conf["end_date"]
        window_mode = "manual"
    else:
        now = pendulum.now("UTC")

        # Use yesterday as the end of the window to avoid partial current-day data
        end = now.subtract(days=1).start_of("day")

        # Rolling 90-day window (inclusive)
        start = end.subtract(days=89)

        start_date = start.to_date_string()
        end_date = end.to_date_string()
        window_mode = "default_last_90_days"

    source = conf.get("source", DEFAULT_SOURCE)
    batch_size = _get_positive_int_conf(conf, "batch_size", default_batch_size)
    max_batches = _get_positive_int_conf(conf, "max_batches", default_max_batches)

    ti = context["ti"]
    ti.xcom_push(key="start_date", value=start_date)
    ti.xcom_push(key="end_date", value=end_date)
    ti.xcom_push(key="source", value=source)
    ti.xcom_push(key="batch_size", value=batch_size)
    ti.xcom_push(key="max_batches", value=max_batches)
    ti.xcom_push(key="window_mode", value=window_mode)


with DAG(
    dag_id="planning_os_weekly",
    description="Weekly orchestration DAG for planning_os ingestion, dbt build, tests, and health checks",
    start_date=datetime(2024, 1, 1),
    # Mondays at 09:00 UTC (01:00 PT / 04:00 ET in standard time; 02:00 PT / 05:00 ET in DST)
    schedule="0 9 * * 1",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["planning_os", "orchestration", "weekly"],
) as dag:

    compute_run_window_task = PythonOperator(
        task_id="compute_run_window",
        python_callable=compute_run_window,
    )

    validate_run_contract_task = PythonOperator(
        task_id="validate_run_contract",
        python_callable=validate_run_contract,
    )

    ingest_iowa_liquor = BashOperator(
        task_id="ingest_iowa_liquor",
        retries=2,
        retry_delay=timedelta(minutes=10),
        bash_command="""
        cd /opt/planning_os && \
        ./run.sh pipeline \
          --source {{ ti.xcom_pull(task_ids='compute_run_window', key='source') }} \
          --start-date {{ ti.xcom_pull(task_ids='compute_run_window', key='start_date') }} \
          --end-date {{ ti.xcom_pull(task_ids='compute_run_window', key='end_date') }} \
                    --batch-size {{ ti.xcom_pull(task_ids='compute_run_window', key='batch_size') }} \
                    --max-batches {{ ti.xcom_pull(task_ids='compute_run_window', key='max_batches') }}
        """,
    )

    validate_data_contract_task = PythonOperator(
        task_id="validate_data_contract",
        python_callable=validate_data_contract,
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command="""
        cd /opt/planning_os && \
        dbt source freshness
        """,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="""
        cd /opt/planning_os && \
        dbt build
        """,
    )

    dbt_test_critical = BashOperator(
        task_id="dbt_test_critical",
        bash_command="""
        cd /opt/planning_os && \
        dbt test --select tag:critical
        """,
    )

    dbt_test_full = BashOperator(
        task_id="dbt_test_full",
        bash_command="""
        cd /opt/planning_os && \
        dbt test
        """,
    )

    pipeline_health_check = PythonOperator(
        task_id="pipeline_health_check",
        python_callable=check_pipeline_health,
    )

    publish_run_summary = BashOperator(
        task_id="publish_run_summary",
        trigger_rule="all_done",
        bash_command="""
        echo "planning_os run summary" && \
        echo "Window mode: {{ ti.xcom_pull(task_ids='compute_run_window', key='window_mode') }}" && \
        echo "Source:      {{ ti.xcom_pull(task_ids='compute_run_window', key='source') }}" && \
        echo "Start:       {{ ti.xcom_pull(task_ids='compute_run_window', key='start_date') }}" && \
        echo "End:         {{ ti.xcom_pull(task_ids='compute_run_window', key='end_date') }}" && \
        echo "Window days: {{ ti.xcom_pull(task_ids='validate_run_contract', key='window_days') }}" && \
        echo "Batch size:  {{ ti.xcom_pull(task_ids='compute_run_window', key='batch_size') }}" && \
        echo "Max batches: {{ ti.xcom_pull(task_ids='compute_run_window', key='max_batches') }}" && \
        echo "Rows landed: {{ ti.xcom_pull(task_ids='validate_data_contract', key='ingested_row_count') }}" && \
        echo "Landed min:  {{ ti.xcom_pull(task_ids='validate_data_contract', key='ingested_min_order_date') }}" && \
        echo "Landed max:  {{ ti.xcom_pull(task_ids='validate_data_contract', key='ingested_max_order_date') }}" && \
        echo "Load lag h:  {{ ti.xcom_pull(task_ids='validate_data_contract', key='ingested_loaded_at_lag_hours') }}" && \
        echo "Freshness:   {{ ti.xcom_pull(task_ids='pipeline_health_check', key='freshness_status') }}" && \
        echo "Snapshot:    {{ ti.xcom_pull(task_ids='pipeline_health_check', key='snapshot_status') }}"
        """,
    )

    (
        compute_run_window_task
        >> validate_run_contract_task
        >> ingest_iowa_liquor
        >> validate_data_contract_task
        >> dbt_source_freshness
        >> dbt_build
        >> dbt_test_critical
        >> dbt_test_full
        >> pipeline_health_check
        >> publish_run_summary
    )