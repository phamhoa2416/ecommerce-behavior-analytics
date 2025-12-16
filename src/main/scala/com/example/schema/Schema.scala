package com.example.schema

import org.apache.spark.sql.types._

object Schema {
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