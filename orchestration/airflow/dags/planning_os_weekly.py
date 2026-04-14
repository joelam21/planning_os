from __future__ import annotations

from datetime import datetime, timedelta
import os
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DEFAULT_SOURCE = "iowa_liquor"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_MAX_BATCHES = 2000


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

    validate_ingestion = BashOperator(
        task_id="validate_ingestion",
        bash_command="""
        echo "Validate ingestion window landed successfully" && \
        echo "Source: {{ ti.xcom_pull(task_ids='compute_run_window', key='source') }}" && \
        echo "Start:  {{ ti.xcom_pull(task_ids='compute_run_window', key='start_date') }}" && \
        echo "End:    {{ ti.xcom_pull(task_ids='compute_run_window', key='end_date') }}" && \
        echo "Batch:  {{ ti.xcom_pull(task_ids='compute_run_window', key='batch_size') }}" && \
        echo "Max:    {{ ti.xcom_pull(task_ids='compute_run_window', key='max_batches') }}"
        """,
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

    pipeline_health_check = BashOperator(
        task_id="pipeline_health_check",
        bash_command="""
        cd /opt/planning_os && \
        echo "Placeholder: query MON_PIPELINE_HEALTH after Snowflake tooling is available"
        """,
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
        echo "Batch size:  {{ ti.xcom_pull(task_ids='compute_run_window', key='batch_size') }}" && \
        echo "Max batches: {{ ti.xcom_pull(task_ids='compute_run_window', key='max_batches') }}"
        """,
    )

    (
        compute_run_window_task
        >> ingest_iowa_liquor
        >> validate_ingestion
        >> dbt_source_freshness
        >> dbt_build
        >> dbt_test_critical
        >> dbt_test_full
        >> pipeline_health_check
        >> publish_run_summary
    )