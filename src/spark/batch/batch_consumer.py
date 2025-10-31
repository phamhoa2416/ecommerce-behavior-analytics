from pyspark.sql.functions import col, from_json, to_timestamp
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_utils import create_spark_session, get_ecommerce_schema, get_kafka_config


def main():
    spark = create_spark_session("Batch Consumer")
    schema = get_ecommerce_schema()
    kafka_config = get_kafka_config()
    
    kafka_bootstrap_servers = kafka_config["bootstrap_servers"]
    kafka_topic = kafka_config["topic"]

    df_batch = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()


    df_parsed = df_batch.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        "data.*",
        "kafka_timestamp"
    )

    df_final = df_parsed.withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z")
    )

    df_final.show(10, truncate=False)
    print(f"Total records: {df_final.count()}")

    hdfs_output_path = "hdfs://172.19.0.3:9000/data/ecommerce/batch"

    df_final.write \
        .mode("append") \
        .partitionBy("event_type") \
        .orc(hdfs_output_path)

    print(f"Data saved to HDFS: {hdfs_output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
