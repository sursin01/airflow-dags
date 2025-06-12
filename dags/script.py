from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # for SQL
from airflow.providers.standard.operators.bash import BashOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='spark_job_dag',
    default_args=default_args,
    catchup=False,
    description='Run Spark job via BashOperator',
) as dag:

    run_spark_job = BashOperator(
        task_id='run_spark_submit',
        bash_command="""
            spark-submit \
            --master local[*] \
            ./my_spark_job.py
        """
    )
