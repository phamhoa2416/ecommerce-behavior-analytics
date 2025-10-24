from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType


def create_spark_session(app_name="Spark Application"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')
    return spark


def get_ecommerce_schema():
    return StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True)
    ])


def get_kafka_config():
    return {
        "bootstrap_servers": "localhost:9092",
        "topic": "ecommerce_data"
    }
