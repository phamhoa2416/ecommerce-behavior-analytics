package com.example.util

import com.example.AppConfig
import com.example.handler.RetryHandler
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.slf4j.Logger

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object MLUtils {
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

  def loadDataFromMinio(
                       spark: SparkSession,
                       logger: Logger
                       ): DataFrame = {
    try {
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

  def prepareModelData(
                      data: DataFrame,
                      logger: Logger
                      ): Array[Dataset[Row]] = {
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2), seed = 36)

    training.cache()
    test.cache()

    logger.info(s"Training set size: ${training.count()}")
    logger.info(s"Test set size: ${test.count()}")
    Array(training, test)
  }

  def saveModelMetadata(
                         spark: SparkSession,
                         bucketName: String,
                         path: String,
                         logger: Logger,
                         metadata: Map[String, String]
                       ): Unit = {
    import spark.implicits._

    val metadataWithTimestamp = metadata + (
      "saved_at" -> java.time.Instant.now().toString
      )

    val metadataDf = metadataWithTimestamp.toSeq.toDF("key", "value")

    logger.info(s"Saving model metadata to s3a://$bucketName/$path")
    MinioUtils.writeDeltaTable(
      metadataDf,
      bucketName,
      path,
      SaveMode.Overwrite
    )
  }

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
