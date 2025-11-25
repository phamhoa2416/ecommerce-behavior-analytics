package com.example.parser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object Parser {
  def parseData(df: DataFrame, schema: StructType): DataFrame = {
    df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
      )
      .select("data.*")
      .withColumn("event_time", regexp_replace(col("event_time"), lit(" UTC"), lit("")))
      .withColumn(
        "event_time",
        to_utc_timestamp(to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"), "UTC")
      )
  }
}

