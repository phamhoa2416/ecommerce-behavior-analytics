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
}