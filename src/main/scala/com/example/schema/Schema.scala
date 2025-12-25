package com.example.schema

import org.apache.spark.sql.types._

/**
 * Schema definitions for e-commerce event data.
 * 
 * This object provides Spark SQL schema definitions for parsing JSON data from Kafka,
 * including standard event schemas and CDC (Change Data Capture) schemas from Debezium.
 */
object Schema {
  /**
   * Standard schema for e-commerce events from Kafka.
   * 
   * This schema is used for parsing regular JSON events where all fields are strings
   * and will be cast to appropriate types during parsing. All fields are nullable
   * to handle missing data gracefully.
   * 
   * Fields:
   * - event_time: Timestamp of the event (string format)
   * - event_type: Type of event (view, cart, purchase, remove_from_cart)
   * - product_id: Unique identifier for the product
   * - category_id: Unique identifier for the product category
   * - category_code: Human-readable category code
   * - brand: Product brand name
   * - price: Product price (string format, will be cast to double)
   * - user_id: Unique identifier for the user
   * - user_session: Session identifier for the user
   */
  val schema: StructType = StructType(
    Seq(
      StructField("event_time", StringType, nullable = true),
      StructField("event_type", StringType, nullable = true),
      StructField("product_id", StringType, nullable = true),
      StructField("category_id", StringType, nullable = true),
      StructField("category_code", StringType, nullable = true),
      StructField("brand", StringType, nullable = true),
      StructField("price", StringType, nullable = true),
      StructField("user_id", StringType, nullable = true),
      StructField("user_session", StringType, nullable = true)
    )
  )

  /**
   * Schema for CDC (Change Data Capture) events from Debezium.
   * 
   * This schema represents the payload structure of CDC events, where data types
   * are already in their native formats (Long, Binary, etc.) as captured by Debezium.
   * Includes additional CDC metadata fields for tracking changes.
   * 
   * Fields:
   * - id: Database record ID (integer)
   * - event_time: Timestamp in microseconds (long)
   * - event_type: Type of event (string)
   * - product_id: Product identifier (long)
   * - category_id: Category identifier (long)
   * - category_code: Category code (string)
   * - brand: Brand name (string)
   * - price: Price stored as binary (will be converted to decimal)
   * - user_id: User identifier (long)
   * - user_session: Session identifier (string)
   * - __deleted: Debezium deletion flag (string)
   * - __op: Debezium operation type (c=create, u=update, d=delete) (string)
   * - __source_ts_ms: Source timestamp in milliseconds (long)
   */
  val schemaCDC = StructType(
    Seq(
      StructField("id", IntegerType),
      StructField("event_time", LongType),
      StructField("event_type", StringType),
      StructField("product_id", LongType),
      StructField("category_id", LongType),
      StructField("category_code", StringType),
      StructField("brand", StringType),
      StructField("price", BinaryType),
      StructField("user_id", LongType),
      StructField("user_session", StringType),
      StructField("__deleted", StringType),
      StructField("__op", StringType),
      StructField("__source_ts_ms", LongType)
    )
  )

  /**
   * Complete Debezium CDC envelope schema.
   * 
   * This schema represents the full Debezium message structure, which includes:
   * - schema: Metadata about the schema structure
   * - payload: The actual data payload (using schemaCDC)
   * 
   * The schema field contains type information and field metadata, while the payload
   * contains the actual event data conforming to schemaCDC.
   */
  val debeziumCDC = new StructType()
    .add("schema", new StructType()
      .add("type", StringType)
      .add("fields", ArrayType(new StructType()
        .add("type", StringType)
        .add("optional", StringType)
        .add("field", StringType)))
      .add("optional", StringType)
      .add("name", StringType))
    .add("payload", schemaCDC)
}