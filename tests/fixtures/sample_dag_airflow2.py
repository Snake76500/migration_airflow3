"""
Sample Airflow 2 DAG for testing migration rules.
"""

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.datasets import Dataset
from airflow.utils.db import provide_session

raw_dataset = Dataset("s3://bucket/raw_data.csv")

default_args = {
    "owner": "data_team",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

def extract_logic(**kwargs):
    # Old execution date variable
    exec_dt = kwargs["execution_date"]
    print(f"Running extract for date: {exec_dt}")
    return f"processed_{exec_dt}"

@provide_session
def query_db_task(session=None, **kwargs):
    # Direct DB access forbidden in Airflow 3
    results = session.query().all()
    return len(results)

with DAG(
    dag_id="sample_etl_v2",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["migration_test"],
) as dag:

    start = DummyOperator(
        task_id="start",
    )

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_logic,
        provide_context=True,
    )

    bash_task = BashOperator(
        task_id="bash_print",
        bash_command="echo 'Execution Date: {{ execution_date }} Next: {{ next_execution_date }}'",
        outlets=[raw_dataset],
    )

    end = DummyOperator(
        task_id="end",
    )

    start >> extract >> bash_task >> end
