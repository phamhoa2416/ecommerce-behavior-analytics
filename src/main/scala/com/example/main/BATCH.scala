package com.example.main

import com.example.AppConfig
import com.example.handler.{DeadLetterQueueHandler, RetryHandler}
import com.example.lineage.LineageTracker
import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.{MinioUtils, SparkUtils}
import com.example.validation.Validator
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, dayofmonth, month, year}
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object BATCH {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Batch")
    val kafkaOptions = Map(
      "kafka.bootstrap.servers" -> AppConfig.KAFKA_BOOTSTRAP_SERVERS,
      "subscribe" -> AppConfig.KAFKA_BATCH_TOPIC,
      "startingOffsets" -> "earliest",
      "kafka.group.id" -> AppConfig.KAFKA_GROUP_ID
    )

    val minioEndpoint = AppConfig.MINIO_ENDPOINT
    val minioAccessKey = AppConfig.MINIO_ACCESS_KEY
    val minioSecretKey = AppConfig.MINIO_SECRET_KEY
    val minioBucketName = AppConfig.MINIO_BUCKET_NAME
    val minioPathStyleAccess = AppConfig.MINIO_PATH_STYLE_ACCESS

    val rawPath = AppConfig.MINIO_BASE_PATH
    val invalidPath = s"${AppConfig.MINIO_BASE_PATH}/batch/invalid"
    val dlqPath = s"${AppConfig.MINIO_BASE_PATH}/batch/dlq"
    val lineagePath = s"${AppConfig.MINIO_BASE_PATH}/lineage"

    val batchId = System.currentTimeMillis().toString

    try {
      RetryHandler.withRetry(
        MinioUtils.configureMinIO(spark, minioEndpoint, minioAccessKey, minioSecretKey, minioPathStyleAccess),
        name = "MinIO Configuration"
      ) match {
        case Success(_) => logger.info("MinIO configured successfully")
        case Failure(exception) =>
          logger.error("Failed to configure MinIO after retries", exception)
          sys.exit(1)
      }

      RetryHandler.withRetry(
        MinioUtils.checkBucketExists(minioEndpoint, minioAccessKey, minioSecretKey, minioBucketName),
        name = "MinIO Bucket Check/Create"
      ) match {
        case Success(_) => logger.info(s"MinIO bucket '$minioBucketName' is ready.")
        case Failure(exception) =>
          logger.error(s"Error while checking/creating MinIO bucket '$minioBucketName'", exception)
          sys.exit(1)
      }

      logger.info(s"Reading CDC events from Kafka topic (source: PostgreSQL): ${AppConfig.KAFKA_BATCH_TOPIC}")

      val kafkaDf = spark.read
        .format("kafka")
        .options(kafkaOptions)
        .load()

      val rawRecordCount = kafkaDf.count()
      logger.info(s"Total CDC records read from Kafka: $rawRecordCount")

      if (rawRecordCount == 0) {
        logger.warn("No records found in Kafka topic. Exiting batch job.")
        spark.stop()
      }

      val parsedDf = Parser.parse(
        kafkaDf,
        Schema.schema,
        AppConfig.SPARK_TIMESTAMP_PATTERN,
        AppConfig.SPARK_TIMEZONE
      )

      val validationResult = Validator.validateAndClean(parsedDf)
      logger.info(s"Validation metrics: total=${validationResult.metrics.totalRecords}, valid=${validationResult.metrics.validRecords}, invalid=${validationResult.metrics.invalidRecords}")

      Validator.saveInvalidRecords(
        invalidRecords = validationResult.invalidRecords,
        bucketName = minioBucketName,
        path = invalidPath
      )

      val validWithPartition = validationResult.validRecords
        .withColumn("year", year(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))

      logger.info(s"Valid CDC record count in this batch: ${validWithPartition.count()}")

      RetryHandler.withRetry(
        MinioUtils.writeDeltaTable(
          df = validWithPartition,
          bucketName = minioBucketName,
          path = rawPath,
          saveMode = SaveMode.Append,
          partitionColumns = Some(Seq("year", "month", "day")),
          repartitionColumns = Some(Seq("year", "month", "day"))
        ),
        name = "Write Delta Raw Zone"
      ) match {
        case Success(_) =>
          logger.info("Successfully wrote valid records to Delta raw zone")
          LineageTracker.log(
            spark = spark,
            bucketName = minioBucketName,
            lineagePath = lineagePath,
            pipeline = "batch",
            batchId = batchId,
            source = s"Kafka:${AppConfig.KAFKA_BATCH_TOPIC}",
            sink = rawPath,
            metrics = Map(
              "total" -> validationResult.metrics.totalRecords,
              "valid" -> validationResult.metrics.validRecords,
              "invalid" -> validationResult.metrics.invalidRecords
            ),
            status = "success",
            mode = "ingest"
          )
        case Failure(ex) =>
          logger.error("Failed to write valid records after retries, sending to DLQ", ex)
          DeadLetterQueueHandler.writeToDLQ(
            records = validWithPartition,
            path = dlqPath,
            bucketName = minioBucketName,
            batchId = System.currentTimeMillis(),
            reason = s"write_raw_failed: ${ex.getMessage}",
            retryCount = 0
          )
          LineageTracker.log(
            spark = spark,
            bucketName = minioBucketName,
            lineagePath = lineagePath,
            pipeline = "batch",
            batchId = batchId,
            source = s"Kafka:${AppConfig.KAFKA_BATCH_TOPIC}",
            sink = rawPath,
            metrics = Map(
              "total" -> validationResult.metrics.totalRecords,
              "valid" -> validationResult.metrics.validRecords,
              "invalid" -> validationResult.metrics.invalidRecords
            ),
            status = "failed",
            mode = "ingest",
            message = s"write_raw_failed: ${ex.getMessage}"
          )
      }

      logger.info("Batch job completed successfully!")
    } catch {
      case NonFatal(ex) =>
        logger.error("Batch job failed", ex)
        throw ex
    } finally {
      spark.stop()
    }
  }
}

