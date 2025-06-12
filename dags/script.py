from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    catchup=False,
    description='A simple Hello World DAG',
    schedule_interval=None,
) as dag:

    hello_task = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello, World! this is computer abc"'
    )
