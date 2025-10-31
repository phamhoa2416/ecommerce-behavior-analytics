from pyspark.sql.functions import col, from_json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_utils import create_spark_session, get_ecommerce_schema, get_kafka_config

def write_to_clickhouse(batch_df, batch_id):
    clickhouse_url = "jdbc:clickhouse://localhost:8123/default"
    clickhouse_properties = {
        "user": "admin",
        "password": "password",
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }

    batch_df.write \
    .mode("append") \
    .jdbc(url=clickhouse_url, table="ecommerce_data", properties=clickhouse_properties)


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

    query = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_clickhouse) \
        .option("checkpointLocation", "/tmp/checkpoint_clickhouse") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
