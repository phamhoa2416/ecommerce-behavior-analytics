from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, regexp_replace
from pyspark.sql.types import StructType, StringType, FloatType

# Spark session
spark = SparkSession.builder \
    .appName("ECommerceStream") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# Schema
schema = StructType() \
    .add("user_id", StringType()) \
    .add("event_type", StringType()) \
    .add("product_id", StringType()) \
    .add("category_code", StringType()) \
    .add("brand", StringType()) \
    .add("price", FloatType()) \
    .add("timestamp", StringType())

# Đọc dữ liệu từ Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce_behavior") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Chuẩn hoá timestamp
# Xoá " UTC" và convert sang kiểu timestamp Spark hiểu được
clean_df = parsed_df \
    .withColumn("timestamp", regexp_replace("timestamp", " UTC", "")) \
    .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")) \
    .filter(col("event_type").isNotNull())

# Ghi batch vào ClickHouse
def write_to_clickhouse(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/ecommerce") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "user_behavior_realtime") \
        .option("user", "admin") \
        .option("password", "password") \
        .mode("append") \
        .save()

# Ghi stream
query = clean_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .outputMode("append") \
    .start()

query.awaitTermination()
