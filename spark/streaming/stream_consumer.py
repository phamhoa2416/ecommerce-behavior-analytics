from pyspark.sql.functions import col, from_json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_utils import create_spark_session, get_ecommerce_schema, get_kafka_config


def main():
    spark = create_spark_session("Structured Streaming Consumer")
    schema = get_ecommerce_schema()
    kafka_config = get_kafka_config()
    
    kafka_bootstrap_servers = kafka_config["bootstrap_servers"]
    kafka_topic = kafka_config["topic"]

    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    df_parsed = df_stream.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        "data.*",
        "kafka_timestamp"
    )

    # Ghi dữ liệu ra console (streaming output)
    query = df_parsed.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
