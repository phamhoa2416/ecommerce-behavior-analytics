package com.example.util

import com.example.config.AppConfig
import org.apache.spark.sql.SparkSession

/**
 * Utility object for creating and configuring Spark sessions.
 * Provides a standardized way to create SparkSession instances with Delta Lake support
 * and optimized configurations for e-commerce data processing.
 */
object SparkUtils {
  /**
   * Creates a SparkSession with Delta Lake support and optimized configurations.
   * 
   * This function creates a SparkSession configured with:
   * - Delta Lake extensions for table operations
   * - Adaptive query execution enabled for better performance
   * - Partition coalescing for optimized shuffling
   * - Shuffle partitions configured from AppConfig
   * 
   * @param appName The application name for the Spark session
   * @return Configured SparkSession instance ready for Delta Lake operations
   */
  def createSparkSession(appName: String): SparkSession = {
    // Create SparkSession with Delta Lake extensions and adaptive execution
    val spark = SparkSession.builder()
      .appName(appName)
      .master(AppConfig.SPARK_MASTER)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Set shuffle partitions for optimal performance
    spark.conf.set("spark.sql.shuffle.partitions", AppConfig.SPARK_SHUFFLE_PARTITIONS)
    spark
  }
}
