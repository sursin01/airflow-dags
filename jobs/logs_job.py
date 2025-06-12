from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import col, from_json
import logging

def main():
    topic = "logs.raw.test"
    bucket_name = "log.analytics"
    delta_path = f"s3a://{bucket_name}/logs2"

    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("LogAnalyticsJob")
    logger.info("Starting Spark Session...")

    spark = SparkSession.builder \
        .appName("LogAnalyticsJob") \
        .getOrCreate()

    logger.info("Reading from Kafka topic: %s", topic)

    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "10.0.0.55:31403")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("kafka.group.id", "my-spark-consumer-group")
        .load()
    )

    schema = StructType() \
        .add("user_id", IntegerType()) \
        .add("message", StringType()) \
        .add("level", StringType()) \
        .add("timestamp", StringType())

    logger.info("Parsing JSON payloads...")

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    logger.info("Filtering logs by level: WARN, ERROR")

    filtered_df = parsed_df.filter(col("level").isin("WARN", "ERROR"))

    logger.info("Writing filtered logs to Delta at %s", delta_path)

    filtered_df.write.format("delta").mode("append").save(delta_path)

    logger.info("Job completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
