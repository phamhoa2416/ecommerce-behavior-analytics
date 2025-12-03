package com.example.parser

import com.example.schema.EcommerceEvent
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object Parser {
  private val DefaultTimestampPattern = "yyyy-MM-dd HH:mm:ss"
  private val DefaultTimezone = "UTC"

  def parseData(
    df: DataFrame,
    schema: StructType,
    timestampPattern: String = DefaultTimestampPattern,
    timezone: String = DefaultTimezone
  ): Dataset[EcommerceEvent] =
    parseToEcommerceEvents(df, schema, timestampPattern, timezone)

  def parseToEcommerceEvents(
    df: DataFrame,
    schema: StructType,
    timestampPattern: String = DefaultTimestampPattern,
    timezone: String = DefaultTimezone
  ): Dataset[EcommerceEvent] = {
    val parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
      )
      .select("data.*")
      .withColumn("product_id", col("product_id").cast("long"))
      .withColumn("category_id", col("category_id").cast("long"))
      .withColumn("price", col("price").cast("double"))
      .withColumn("user_id", col("user_id").cast("long"))
      .withColumn("event_time", regexp_replace(col("event_time"), lit(" UTC"), lit("")))
      .withColumn(
        "event_time",
        to_utc_timestamp(to_timestamp(col("event_time"), timestampPattern), timezone)
      )

    import df.sparkSession.implicits._
    parsed.as[EcommerceEvent]
  }
}

