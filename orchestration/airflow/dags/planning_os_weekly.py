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

PIPELINE_FAILURE_TASK_IDS = [
    "validate_run_contract",
    "ingest_iowa_liquor",
    "validate_data_contract",
    "dbt_source_freshness",
    "dbt_build",
    "dbt_test_critical",
    "dbt_test_full",
    "pipeline_health_check",
]

FINAL_STATUS_TASK_IDS = PIPELINE_FAILURE_TASK_IDS + ["persist_run_summary"]


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


def _get_failed_task_ids(dag_run: Any, task_ids: list[str]) -> list[str]:
    failed_task_ids: list[str] = []

    if not dag_run:
        return failed_task_ids

    for task_id in task_ids:
        try:
            task_instance = dag_run.get_task_instance(task_id)
        except Exception:
            continue

        if task_instance and task_instance.state in ("failed", "upstream_failed"):
            failed_task_ids.append(task_id)

    return failed_task_ids


ALERT_EMAIL_ENV = "PLANNING_OS_ALERT_EMAIL"


def _get_alert_email() -> str | None:
    return os.getenv(ALERT_EMAIL_ENV)


def _build_failure_email_body(context: dict[str, Any]) -> tuple[str, str]:
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    logical_date = str(context.get("logical_date", ""))
    exception = str(context.get("exception", ""))

    ti = context["task_instance"]
    source = ti.xcom_pull(task_ids="compute_run_window", key="source") or "—"
    start_date = ti.xcom_pull(task_ids="compute_run_window", key="start_date") or "—"
    end_date = ti.xcom_pull(task_ids="compute_run_window", key="end_date") or "—"
    window_mode = ti.xcom_pull(task_ids="compute_run_window", key="window_mode") or "—"

    log_url = context["task_instance"].log_url

    subject = f"[planning_os] FAILED: {dag_id} / {task_id} ({run_id})"
    body = (
        f"<h3>Task failed: {task_id}</h3>"
        f"<table>"
        f"<tr><td><b>DAG</b></td><td>{dag_id}</td></tr>"
        f"<tr><td><b>Task</b></td><td>{task_id}</td></tr>"
        f"<tr><td><b>Run ID</b></td><td>{run_id}</td></tr>"
        f"<tr><td><b>Logical date</b></td><td>{logical_date}</td></tr>"
        f"<tr><td><b>Window mode</b></td><td>{window_mode}</td></tr>"
        f"<tr><td><b>Source</b></td><td>{source}</td></tr>"
        f"<tr><td><b>Start date</b></td><td>{start_date}</td></tr>"
        f"<tr><td><b>End date</b></td><td>{end_date}</td></tr>"
        f"<tr><td><b>Exception</b></td><td><pre>{exception}</pre></td></tr>"
        f"<tr><td><b>Logs</b></td><td><a href=\"{log_url}\">{log_url}</a></td></tr>"
        f"</table>"
    )
    return subject, body


def failure_callback(context: dict[str, Any]) -> None:
    from airflow.utils.email import send_email

    recipient = _get_alert_email()
    if not recipient:
        print(
            f"[alert] No {ALERT_EMAIL_ENV} configured — skipping failure email. "
            f"Task: {context['task_instance'].task_id}"
        )
        return

    subject, body = _build_failure_email_body(context)
    try:
        send_email(to=recipient, subject=subject, html_content=body)
        print(f"[alert] Failure email sent to {recipient}: {subject}")
    except Exception as exc:
        print(f"[alert] Failed to send failure email: {exc}")


def _format_health_status(value: str) -> str:
    """Render a health status value with a visual flag when degraded."""
    upper = value.upper()
    if upper == "WARN":
        return f"<span style=\"color:#b45309;font-weight:bold\">⚠ {value}</span>"
    if upper == "ERROR":
        return f"<span style=\"color:#b91c1c;font-weight:bold\">✖ {value}</span>"
    return f"<span style=\"color:#15803d\">{value}</span>"


def success_callback_scheduled_only(context: dict[str, Any]) -> None:
    """Sends a success notification only for scheduled runs, not manual triggers."""
    from airflow.utils.email import send_email

    dag_run = context.get("dag_run")
    run_type = getattr(dag_run, "run_type", None)
    if run_type != "scheduled":
        return

    recipient = _get_alert_email()
    if not recipient:
        return

    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    run_id = context["run_id"]
    logical_date = str(context.get("logical_date", ""))
    ti = context["task_instance"]

    source = ti.xcom_pull(task_ids="compute_run_window", key="source") or "—"
    start_date = ti.xcom_pull(task_ids="compute_run_window", key="start_date") or "—"
    end_date = ti.xcom_pull(task_ids="compute_run_window", key="end_date") or "—"
    row_count = ti.xcom_pull(task_ids="validate_data_contract", key="ingested_row_count") or "—"
    freshness = ti.xcom_pull(task_ids="pipeline_health_check", key="freshness_status") or "—"
    snapshot = ti.xcom_pull(task_ids="pipeline_health_check", key="snapshot_status") or "—"

    any_warn = any(s.upper() == "WARN" for s in (freshness, snapshot) if s != "—")
    status_label = "OK (DEGRADED)" if any_warn else "OK"
    subject = f"[planning_os] {status_label}: {dag_id} ({run_id})"
    body = (
        f"<h3>Scheduled run completed — {status_label}</h3>"
        f"<table>"
        f"<tr><td><b>DAG</b></td><td>{dag_id}</td></tr>"
        f"<tr><td><b>Run ID</b></td><td>{run_id}</td></tr>"
        f"<tr><td><b>Logical date</b></td><td>{logical_date}</td></tr>"
        f"<tr><td><b>Source</b></td><td>{source}</td></tr>"
        f"<tr><td><b>Window</b></td><td>{start_date} → {end_date}</td></tr>"
        f"<tr><td><b>Rows landed</b></td><td>{row_count}</td></tr>"
        f"<tr><td><b>Freshness status</b></td><td>{_format_health_status(freshness)}</td></tr>"
        f"<tr><td><b>Snapshot status</b></td><td>{_format_health_status(snapshot)}</td></tr>"
        f"</table>"
    )

    try:
        send_email(to=recipient, subject=subject, html_content=body)
        print(f"[alert] Scheduled success email sent to {recipient}")
    except Exception as exc:
        print(f"[alert] Failed to send success email: {exc}")


def _get_snowflake_connection_params() -> dict[str, Any]:
    params: dict[str, Any] = {
        "account": _require_env("DBT_ACCOUNT"),
        "user": _require_env("DBT_USER"),
        "password": _require_env("DBT_PASSWORD"),
        "role": _require_env("DBT_ROLE"),
        "warehouse": _require_env("DBT_WAREHOUSE"),
        "database": _require_env("DBT_DATABASE"),
        "schema": _require_env("DBT_DEV_SCHEMA"),
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
            min(date) as min_order_date,
            max(date) as max_order_date,
            max(loaded_at) as max_loaded_at,
            datediff('hour', max(loaded_at), current_timestamp()) as loaded_at_lag_hours
        from {database}.{schema}.RAW_IOWA_LIQUOR
        where date between to_date(%s) and to_date(%s)
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

    # Warn vs fail policy:
    #   ERROR  → fail the task immediately (blocks downstream, triggers failure alert)
    #   WARN   → log and continue (pipeline is degraded but not broken)
    #   PASS   → nominal
    ti = context["ti"]
    ti.xcom_push(key="freshness_status", value=str(freshness_status))
    ti.xcom_push(key="snapshot_status", value=str(snapshot_status))

    health_summary = (
        f"freshness_status={freshness_status}, snapshot_status={snapshot_status}, "
        f"raw_lag_days={raw_lag_days}, snapshot_lag_days={snapshot_lag_days}, "
        f"fact_row_count={fact_row_count}, daily_store_day_count={daily_store_day_count}, "
        f"sku_velocity_row_count={sku_velocity_row_count}, "
        f"replenishment_forecast_row_count={replenishment_forecast_row_count}"
    )

    freshness_upper = str(freshness_status).upper()
    snapshot_upper = str(snapshot_status).upper()

    if freshness_upper == "ERROR" or snapshot_upper == "ERROR":
        raise ValueError(
            f"Pipeline health check FAILED (ERROR status): {health_summary}"
        )

    if freshness_upper == "WARN" or snapshot_upper == "WARN":
        print(f"[health] WARN — pipeline degraded but not broken: {health_summary}")
    else:
        print(f"[health] PASS: {health_summary}")


def persist_run_summary(**context) -> None:
    """
    Persist one row of run metadata to OPS.PIPELINE_RUN_HISTORY.

    Runs with trigger_rule="all_done" so it captures both successful and failed runs.
    Captures all window, ingestion, dbt, and health data from XCom and context.
    Determines final DAG state (success/failed/degraded) and persists to Snowflake.

    Design rationale:
    - Executes even if upstream tasks fail, so no run is lost from history
    - Collects XCom values pushed by tasks (even if they later failed)
    - Infers final state by checking task instance states
    - Logs success/failure of persist operation separately from DAG state
    """
    import snowflake.connector

    dag_run = context.get("dag_run")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "unknown"
    run_id = context.get("run_id", "")
    logical_date = str(context.get("logical_date", ""))
    run_type = getattr(dag_run, "run_type", "unknown") if dag_run else "unknown"

    # DAG timing
    dag_start_ts = None
    dag_end_ts = None
    duration_seconds = None

    if dag_run:
        if dag_run.start_date:
            dag_start_ts = str(dag_run.start_date)
        if dag_run.end_date:
            dag_end_ts = str(dag_run.end_date)
        if dag_run.start_date and dag_run.end_date:
            delta = dag_run.end_date - dag_run.start_date
            duration_seconds = int(delta.total_seconds())

    ti = context["task_instance"]

    # Window and ingestion params
    window_mode = ti.xcom_pull(task_ids="compute_run_window", key="window_mode") or "unknown"
    source = ti.xcom_pull(task_ids="compute_run_window", key="source") or "iowa_liquor"
    start_date = ti.xcom_pull(task_ids="compute_run_window", key="start_date")
    end_date = ti.xcom_pull(task_ids="compute_run_window", key="end_date")
    window_days = ti.xcom_pull(task_ids="validate_run_contract", key="window_days")
    batch_size = ti.xcom_pull(task_ids="compute_run_window", key="batch_size")
    max_batches = ti.xcom_pull(task_ids="compute_run_window", key="max_batches")

    # Ingestion data contract
    ingested_row_count = ti.xcom_pull(task_ids="validate_data_contract", key="ingested_row_count")
    ingested_min_order_date = ti.xcom_pull(task_ids="validate_data_contract", key="ingested_min_order_date")
    ingested_max_order_date = ti.xcom_pull(task_ids="validate_data_contract", key="ingested_max_order_date")
    ingested_loaded_at_lag_hours = ti.xcom_pull(
        task_ids="validate_data_contract", key="ingested_loaded_at_lag_hours"
    )

    # Health check
    freshness_status = ti.xcom_pull(task_ids="pipeline_health_check", key="freshness_status") or "unknown"
    snapshot_status = ti.xcom_pull(task_ids="pipeline_health_check", key="snapshot_status") or "unknown"

    # dbt_build_status
    dbt_build_status = "unknown"
    try:
        if dag_run:
            dbt_build_ti = dag_run.get_task_instance("dbt_build")
            if dbt_build_ti:
                dbt_build_status = dbt_build_ti.state or "unknown"
    except Exception:
        pass

    # Determine final DAG state from the full critical pipeline path.
    failed_tasks = _get_failed_task_ids(dag_run, PIPELINE_FAILURE_TASK_IDS)

    if failed_tasks:
        final_dag_state = "failed"
    elif freshness_status == "WARN" or snapshot_status == "WARN":
        final_dag_state = "degraded"
    else:
        final_dag_state = "success"

    # Snowflake insert
    params = _get_snowflake_connection_params()
    database = params["database"]
    schema = "OPS"

    sql = f"""
        insert into {database}.{schema}.PIPELINE_RUN_HISTORY (
            dag_id, run_id, logical_date, run_type, window_mode, source,
            start_date, end_date, window_days, batch_size, max_batches,
            ingested_row_count, ingested_min_order_date, ingested_max_order_date,
            ingested_loaded_at_lag_hours, freshness_status, snapshot_status,
            dbt_build_status, final_dag_state, dag_start_ts, dag_end_ts, duration_seconds
        ) values (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s
        )
    """

    values = (
        dag_id,
        run_id,
        logical_date,
        run_type,
        window_mode,
        source,
        start_date,
        end_date,
        window_days,
        batch_size,
        max_batches,
        ingested_row_count,
        ingested_min_order_date,
        ingested_max_order_date,
        ingested_loaded_at_lag_hours,
        freshness_status,
        snapshot_status,
        dbt_build_status,
        final_dag_state,
        dag_start_ts,
        dag_end_ts,
        duration_seconds,
    )

    try:
        with snowflake.connector.connect(**params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, values)
        print(
            f"[ops] Run history persisted: run_id={run_id}, state={final_dag_state}, "
            f"duration={duration_seconds}s, rows={ingested_row_count}"
        )
    except Exception as exc:
        print(f"[ops] Failed to persist run history: {exc}")
        raise


def publish_run_summary(**context) -> None:
    dag_run = context.get("dag_run")
    ti = context["task_instance"]

    summary_lines = [
        "planning_os run summary",
        f"Window mode: {ti.xcom_pull(task_ids='compute_run_window', key='window_mode')}",
        f"Source:      {ti.xcom_pull(task_ids='compute_run_window', key='source')}",
        f"Start:       {ti.xcom_pull(task_ids='compute_run_window', key='start_date')}",
        f"End:         {ti.xcom_pull(task_ids='compute_run_window', key='end_date')}",
        f"Window days: {ti.xcom_pull(task_ids='validate_run_contract', key='window_days')}",
        f"Batch size:  {ti.xcom_pull(task_ids='compute_run_window', key='batch_size')}",
        f"Max batches: {ti.xcom_pull(task_ids='compute_run_window', key='max_batches')}",
        f"Rows landed: {ti.xcom_pull(task_ids='validate_data_contract', key='ingested_row_count')}",
        f"Landed min:  {ti.xcom_pull(task_ids='validate_data_contract', key='ingested_min_order_date')}",
        f"Landed max:  {ti.xcom_pull(task_ids='validate_data_contract', key='ingested_max_order_date')}",
        f"Load lag h:  {ti.xcom_pull(task_ids='validate_data_contract', key='ingested_loaded_at_lag_hours')}",
        f"Freshness:   {ti.xcom_pull(task_ids='pipeline_health_check', key='freshness_status')}",
        f"Snapshot:    {ti.xcom_pull(task_ids='pipeline_health_check', key='snapshot_status')}",
    ]

    print("\n".join(summary_lines))

    failed_tasks = _get_failed_task_ids(dag_run, FINAL_STATUS_TASK_IDS)
    if failed_tasks:
        raise ValueError(
            "planning_os_weekly completed summary publication, but critical tasks failed: "
            f"{', '.join(failed_tasks)}"
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
    on_failure_callback=failure_callback,
    on_success_callback=success_callback_scheduled_only,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": failure_callback,
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

    persist_run_summary_task = PythonOperator(
        task_id="persist_run_summary",
        python_callable=persist_run_summary,
        trigger_rule="all_done",
    )

    publish_run_summary = PythonOperator(
        task_id="publish_run_summary",
        python_callable=publish_run_summary,
        trigger_rule="all_done",
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
        >> persist_run_summary_task
        >> publish_run_summary
    )