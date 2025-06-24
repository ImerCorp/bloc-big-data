from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

# Simple DAG that does nothing
dag = DAG(
    'do_nothing_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
)

# Single task that does nothing
do_nothing = DummyOperator(
    task_id='do_nothing',
    dag=dag,
)
