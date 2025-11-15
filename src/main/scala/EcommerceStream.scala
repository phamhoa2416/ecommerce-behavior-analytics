import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object EcommerceStream {
  def main(args: Array[String]): Unit = {
    println("E-commerce streaming application started.")

    val spark = SparkSession.builder()
      .appName("Ecommerce Streaming Application")
      .getOrCreate()

    val schema = new StructType()
      .add("user_id", StringType)
      .add("event_type", StringType)
      .add("product_id", StringType)
      .add("category_code", StringType)
      .add("brand", StringType)
      .add("price", FloatType)
      .add("timestamp", StringType)

    val rawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ecommerce_behavior")
      .option("startingOffsets", "earliest")
      .load()

    val parsedDF = rawDF
      .select(from_json(col("value").cast("string"), schema).alias("data"))
      .select("data.*")

    val cleanDF = parsedDF
      .withColumn("timestamp", regexp_replace(col("timestamp"), " UTC", ""))
      .withColumn("timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
      .filter(col("event_type").isNotNull)

    def writeToClickHouse(df: DataFrame, batchId: Long): Unit = {
      df.write
        .format("jdbc")
        .option("url", "jdbc:clickhouse://clickhouse:8123/ecommerce")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", "user_behavior_realtime")
        .option("user", "admin")
        .option("password", "password")
        .mode("append")
        .save()
    }

    val query = cleanDF.writeStream
      .foreachBatch(writeToClickHouse _)
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}