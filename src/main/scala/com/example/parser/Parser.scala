package com.example.parser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Parser utility for converting Kafka JSON messages to structured DataFrames.
 * 
 * This object provides functions to parse JSON data from Kafka topics, handling both
 * standard event formats and CDC (Change Data Capture) formats from Debezium.
 * Includes type casting, timestamp parsing, and data transformation logic.
 */
object Parser {
  private val logger = LoggerFactory.getLogger(getClass)

  /** Default timestamp pattern for parsing event_time strings */
  private val DefaultTimestampPattern = "yyyy-MM-dd HH:mm:ss"
  
  /** Default timezone for timestamp parsing */
  private val DefaultTimezone = "UTC"

  /**
   * Parses standard Kafka JSON messages into a structured DataFrame.
   * 
   * This function parses JSON strings from Kafka's "value" column, applies the provided
   * schema, and performs type conversions. It handles:
   * - JSON parsing from string format
   * - Type casting (product_id, category_id, price, user_id to numeric types)
   * - Timestamp parsing with timezone handling
   * - UTC suffix removal from timestamps
   * 
   * @param df Input DataFrame containing Kafka messages with a "value" column
   * @param schema The StructType schema to apply for JSON parsing
   * @param timestampPattern Pattern for parsing timestamp strings (default: "yyyy-MM-dd HH:mm:ss")
   * @param timezone Timezone for timestamp parsing (default: "UTC")
   * @return DataFrame with parsed and typed columns
   * @throws Exception if parsing fails
   */
  def parse(
             df: DataFrame,
             schema: StructType,
             timestampPattern: String = DefaultTimestampPattern,
             timezone: String = DefaultTimezone
           ): DataFrame = {
    logger.info("Parsing Kafka JSON data")

    try {
      // Parse JSON from Kafka value column and extract all fields
      df.select(
          from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        // Cast string fields to appropriate numeric types
        .withColumn("product_id", col("product_id").cast("long"))
        .withColumn("category_id", col("category_id").cast("long"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("user_id", col("user_id").cast("long"))
        // Remove UTC suffix from timestamp strings before parsing
        .withColumn("event_time", regexp_replace(col("event_time"), lit(" UTC"), lit("")))
        // Parse timestamp string to TimestampType
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

  /**
   * Parses Debezium CDC (Change Data Capture) JSON messages into a structured DataFrame.
   * 
   * This function handles the Debezium envelope format, extracting the payload and
   * performing specialized conversions:
   * - Extracts data from the Debezium payload structure
   * - Converts microsecond timestamps to Spark TimestampType
   * - Converts binary-encoded price (from database) to decimal format
   * - Filters to only include standard event columns (excludes CDC metadata)
   * 
   * The price conversion assumes the binary value represents an integer in cents,
   * which is converted to dollars with 2 decimal places.
   * 
   * @param df Input DataFrame containing Debezium CDC messages with a "value" column
   * @param schema The StructType schema for the Debezium envelope (typically debeziumCDC)
   * @param timestampPattern Pattern for timestamp parsing (not used for CDC, kept for API consistency)
   * @param timezone Timezone for timestamp parsing (not used for CDC, kept for API consistency)
   * @return DataFrame with parsed event data, excluding CDC metadata fields
   * @throws Exception if parsing fails
   */
  def parseCDC(
                df: DataFrame,
                schema: StructType,
                timestampPattern: String = DefaultTimestampPattern,
                timezone: String = DefaultTimezone
              ): DataFrame = {
    logger.info("Parsing Kafka CDC JSON data")

    try {
      // Parse Debezium envelope JSON and extract payload
      df.select(
          from_json(col("value").cast("string"), schema).alias("data")
        )
        .select("data.payload.*")
        // Convert microsecond timestamp to Spark TimestampType
        .withColumn(
          "event_time",
          timestamp_micros(col("event_time"))
        )
        // Convert binary price to decimal: hex -> int (cents) -> dollars with 2 decimals
        .withColumn(
          "price",
          (conv(hex(col("price")), 16, 10)
            .cast("int") / 100.0)
            .cast("decimal(10, 2)")
        )
        // Select only standard event columns, excluding CDC metadata
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

