from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hello_planning_os",
    description="Minimal verification DAG for planning_os Airflow setup",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["planning_os", "verification"],
) as dag:

    show_repo = BashOperator(
        task_id="show_repo",
        bash_command="cd /opt/planning_os && pwd && ls",
    )

    show_airflow_dags = BashOperator(
        task_id="show_airflow_dags",
        bash_command="ls /opt/airflow/dags",
    )

    show_repo >> show_airflow_dags