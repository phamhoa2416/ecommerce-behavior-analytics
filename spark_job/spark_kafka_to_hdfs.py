from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True)
])

def main():
    spark = SparkSession.builder \
        .appName("KafkaToHDFS") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    parsed = df.selectExpr("CAST(value AS STRING) as json") \
               .select(from_json(col("json"), schema).alias("data")) \
               .select("data.*")

    result = parsed \
        .withColumn("event_date", to_date("event_time")) \
        .groupBy("event_date", "event_type") \
        .agg(count("*").alias("cnt"), sum("price").alias("revenue"))

    result.write \
        .mode("append") \
        .partitionBy("event_date") \
        .parquet("hdfs://namenode:9000/output/events_daily")

    print("Done!")
    spark.stop()

if __name__ == "__main__":
    main()