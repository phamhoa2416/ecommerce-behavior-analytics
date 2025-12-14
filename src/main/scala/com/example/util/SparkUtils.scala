package com.example.util

import com.example.config.AppConfig
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def createSparkSession(appName: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master(AppConfig.SPARK_MASTER)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", AppConfig.SPARK_SHUFFLE_PARTITIONS)
    spark
  }
}
