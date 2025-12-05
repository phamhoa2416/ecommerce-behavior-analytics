package com.example.parser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

object Parser {
  private val logger = LoggerFactory.getLogger(getClass)

  private val DefaultTimestampPattern = "yyyy-MM-dd HH:mm:ss"
  private val DefaultTimezone = "UTC"

  def parse(
           df: DataFrame,
           schema: StructType,
            timestampPattern: String = DefaultTimestampPattern,
            timezone: String = DefaultTimezone
           ): DataFrame = {
    logger.info("Parsing Kafka JSON data")

    try {
      df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"))
        .select("data.*", "kafka_timestamp")
        .withColumn("product_id", col("product_id").cast("long"))
        .withColumn("category_id", col("category_id").cast("long"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("user_id", col("user_id").cast("long"))
        .withColumn("event_time", regexp_replace(col("event_time"), lit(" UTC"), lit("")))
        .withColumn(
          "event_time",
          to_timestamp(col("event_time"), timestampPattern)
        )
    } catch {
      case e: Exception =>
        logger.error("Error parsing Kafka data", e)
        throw e
    }
  }
}

