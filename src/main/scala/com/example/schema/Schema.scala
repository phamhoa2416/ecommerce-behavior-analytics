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
   * This schema represents the data structure within before/after fields of CDC events.
   * Data types are in their native formats (Long, String, etc.) as captured by Debezium.
   * 
   * Fields:
   * - id: Database record ID (integer)
   * - event_time: Timestamp in microseconds (long)
   * - event_type: Type of event (string)
   * - product_id: Product identifier (long)
   * - category_id: Category identifier (long)
   * - category_code: Category code (string, nullable)
   * - brand: Brand name (string)
   * - price: Price stored as base64-encoded binary string (will be converted to decimal)
   * - user_id: User identifier (long)
   * - user_session: Session identifier (string)
   */
  val schemaCDC = StructType(
    Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("event_time", LongType, nullable = true),
      StructField("event_type", StringType, nullable = true),
      StructField("product_id", LongType, nullable = true),
      StructField("category_id", LongType, nullable = true),
      StructField("category_code", StringType, nullable = true),
      StructField("brand", StringType, nullable = true),
      StructField("price", StringType, nullable = true), // Base64-encoded binary in JSON
      StructField("user_id", LongType, nullable = true),
      StructField("user_session", StringType, nullable = true)
    )
  )

  /**
   * Schema for Debezium source metadata.
   * Contains information about the source database and transaction.
   */
  val debeziumSource = StructType(
    Seq(
      StructField("version", StringType, nullable = true),
      StructField("connector", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("ts_ms", LongType, nullable = true),
      StructField("snapshot", StringType, nullable = true),
      StructField("db", StringType, nullable = true),
      StructField("sequence", StringType, nullable = true),
      StructField("schema", StringType, nullable = true),
      StructField("table", StringType, nullable = true),
      StructField("txId", LongType, nullable = true),
      StructField("lsn", LongType, nullable = true),
      StructField("xmin", LongType, nullable = true)
    )
  )

  /**
   * Complete Debezium CDC envelope schema.
   * 
   * This schema represents the actual Debezium message structure from PostgreSQL connector:
   * - before: Previous state of the record (null for inserts/reads, contains data for updates/deletes)
   * - after: New state of the record (null for deletes, contains data for inserts/updates/reads)
   * - source: Metadata about the source database and transaction
   * - op: Operation type (c=create, u=update, d=delete, r=read/snapshot)
   * - ts_ms: Timestamp in milliseconds
   * - transaction: Transaction metadata (nullable)
   */
  val debeziumCDC = new StructType()
    .add("before", schemaCDC, nullable = true)
    .add("after", schemaCDC, nullable = true)
    .add("source", debeziumSource, nullable = true)
    .add("op", StringType, nullable = true) // c=create, u=update, d=delete, r=read
    .add("ts_ms", LongType, nullable = true)
    .add("transaction", StringType, nullable = true)
}