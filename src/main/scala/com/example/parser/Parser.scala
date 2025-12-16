package com.example.parser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, DecimalType, LongType, StructType}
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
          from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
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

  def parseCDC(
                df: DataFrame,
                schema: StructType,
                timestampPattern: String = DefaultTimestampPattern,
                timezone: String = DefaultTimezone
              ): DataFrame = {
    logger.info("Parsing Kafka CDC JSON data")

    try {
      df.select(
          from_json(col("value").cast("string"), schema).alias("data")
        )
        .select("data.payload.*")
        .withColumn(
          "event_time",
          timestamp_micros(col("event_time"))
        )
        .withColumn(
          "price",
          (conv(hex(col("price")), 16, 10)
            .cast("int") / 100.0)
            .cast("decimal(10, 2)")
        )
        .select(
          col("event_time"),
          col("event_type"),
          col("product_id"),
          col("category_id"),
          col("category_code"),
          col("brand"),
          col("price"),
          col("user_id"),
          col("user_session")
        )
    } catch {
      case e: Exception =>
        logger.error("Error parsing Kafka CDC data", e)
        throw e
    }
  }
}

