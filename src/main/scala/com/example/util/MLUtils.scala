package com.example.util

import com.example.config.AppConfig
import com.example.handler.RetryHandler
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.slf4j.Logger

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
 * Utility object for machine learning operations.
 * Provides functions for data loading, model metadata management, and time formatting.
 */
object MLUtils {
  /**
   * Formats a duration in seconds into a human-readable string.
   * 
   * Converts seconds to a format like "1h 23m 45.678s", omitting zero components.
   * 
   * @param secs The duration in seconds (can be fractional)
   * @return Formatted string representation (e.g., "1h 23m 45.678s" or "0.0s")
   */
  def parseTime(secs: Double): String = {
    val h = secs.toInt / 3600
    val m = (secs - h * 3600.0).toInt / 60
    val s = secs - h * 3600.0 - m * 60

    val parts = Seq(
      if (h > 0) Some(f"${h}h") else None,
      if (m > 0) Some(f"${m}m") else None,
      if (s > 0) Some(f"$s%.3fs") else None
    ).flatten

    if (parts.isEmpty) "0.0s" else parts.mkString(" ")
  }

  /**
   * Loads data from MinIO Delta table with retry logic and error handling.
   * 
   * This function configures MinIO connection, reads a Delta table, and caches the result.
   * It uses RetryHandler to handle transient failures and exits the application if
   * configuration or data loading fails after retries.
   * 
   * @param spark The SparkSession instance
   * @param logger The logger instance for logging operations
   * @return DataFrame containing the loaded data from MinIO
   * @throws Exception if MinIO configuration or data loading fails after retries
   */
  def loadDataFromMinio(
                       spark: SparkSession,
                       logger: Logger
                       ): DataFrame = {
    try {
      // Configure MinIO connection with retry logic
      RetryHandler.withRetry(
        MinioUtils.configureMinIO(
          spark = spark,
          endpoint = AppConfig.MINIO_ENDPOINT,
          accessKey = AppConfig.MINIO_ACCESS_KEY,
          secretKey = AppConfig.MINIO_SECRET_KEY,
          pathStyleAccess = AppConfig.MINIO_PATH_STYLE_ACCESS
        ),
        name = "MinIO Configuration"
      ) match {
        case Success(_) => logger.info("MinIO configured successfully")
        case Failure(exception) =>
          logger.error("Failed to configure MinIO after retries", exception)
          sys.exit(1)
      }

      // Load and cache data from Delta table with retry logic
      val data: DataFrame = RetryHandler.withRetry(
        MinioUtils.readDeltaTable(
          spark,
          bucketName = AppConfig.MINIO_BUCKET_NAME,
          path = AppConfig.MINIO_BASE_PATH
        ).cache(),
        name = "Read Delta Table from MinIO"
      ) match {
        case Success(df) =>
          logger.info("Successfully read data from Delta table in MinIO.")
          df
        case Failure(exception) =>
          logger.error("Failed to read Delta table from MinIO after retries", exception)
          sys.exit(1)
      }

      logger.info(s"Loaded ${data.count()} records from MinIO Delta table")
      data
    } catch {
      case NonFatal(ex) =>
        logger.error("An error occurred during model training.", ex)
        throw ex
    }
  }

  /**
   * Splits data into training and test sets for model evaluation.
   * 
   * This function randomly splits the input data into 80% training and 20% test sets
   * with a fixed random seed for reproducibility. Both datasets are cached for performance.
   * 
   * @param data The input DataFrame to split
   * @param logger The logger instance for logging operations
   * @return Array containing [training Dataset, test Dataset]
   */
  def prepareModelData(
                      data: DataFrame,
                      logger: Logger
                      ): Array[Dataset[Row]] = {
    // Split data into 80% training and 20% test with fixed seed for reproducibility
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2), seed = 36)

    // Cache datasets for multiple uses during training and evaluation
    training.cache()
    test.cache()

    logger.info(s"Training set size: ${training.count()}")
    logger.info(s"Test set size: ${test.count()}")
    Array(training, test)
  }

  /**
   * Saves model metadata to MinIO as a Delta table.
   * 
   * This function stores key-value pairs of model metadata (e.g., model version, accuracy,
   * training parameters) along with a timestamp indicating when it was saved.
   * 
   * @param spark The SparkSession instance
   * @param bucketName The MinIO bucket name where metadata will be stored
   * @param path The path within the bucket for storing metadata
   * @param logger The logger instance for logging operations
   * @param metadata Map of key-value pairs representing model metadata
   */
  def saveModelMetadata(
                         spark: SparkSession,
                         bucketName: String,
                         path: String,
                         logger: Logger,
                         metadata: Map[String, String]
                       ): Unit = {
    import spark.implicits._

    // Add timestamp to metadata
    val metadataWithTimestamp = metadata + (
      "saved_at" -> java.time.Instant.now().toString
      )

    // Convert metadata map to DataFrame with key-value columns
    val metadataDf = metadataWithTimestamp.toSeq.toDF("key", "value")

    logger.info(s"Saving model metadata to s3a://$bucketName/$path")
    MinioUtils.writeDeltaTable(
      metadataDf,
      bucketName,
      path,
      SaveMode.Overwrite
    )
  }

  /**
   * Loads model metadata from MinIO Delta table.
   * 
   * This function reads a Delta table containing key-value pairs and converts it
   * back into a Map for easy access to model metadata.
   * 
   * @param spark The SparkSession instance
   * @param bucketName The MinIO bucket name containing the metadata
   * @param path The path within the bucket where metadata is stored
   * @return Map of key-value pairs representing model metadata
   */
  def loadModelMetadata(
                         spark: SparkSession,
                         bucketName: String,
                         path: String
                       ): Map[String, String] = {
    import spark.implicits._

    MinioUtils.readDeltaTable(spark, bucketName, path)
      .as[(String, String)]
      .collect()
      .toMap
  }
}
