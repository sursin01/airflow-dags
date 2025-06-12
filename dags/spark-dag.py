from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG(
    dag_id='spark_s3_job_example',
    default_args=default_args,
    schedule_interval=None,
    description='Submit Spark job from S3 using SparkSubmitOperator',
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application="s3a://my-spark-jobs-bucket/pi.py",  # S3 path to your spark job
        conn_id='spark_default',  # Define this in Airflow Connections (e.g., Spark master URL)
        application_args=[],      # Args to your spark job if any
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.access.key": "yXdkVN7a1fJ0C18AbnRP",
            "spark.hadoop.fs.s3a.secret.key": "fkYDTqHrgYogctfQcQuDOGNRqb5jKbqisGNOsark",
            "spark.hadoop.fs.s3a.endpoint": "http://172.16.200.34:31713",
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,",
        verbose=True,
    )
