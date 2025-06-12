from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import  SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='example_spark_submit_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Submit Spark job using SparkSubmitOperator'
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_app',
        application='s3a://airflow/jobs/logs_job.py',  # or local path
        conn_id='spark_default',  # defined in Airflow Connections
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": "yXdkVN7a1fJ0C18AbnRP",
            "spark.hadoop.fs.s3a.secret.key": "fkYDTqHrgYogctfQcQuDOGNRqb5jKbqisGNOsark",
            "spark.hadoop.fs.s3a.endpoint": "http://172.16.200.34:31713",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        application_args=[],
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
        verbose=True
    )
