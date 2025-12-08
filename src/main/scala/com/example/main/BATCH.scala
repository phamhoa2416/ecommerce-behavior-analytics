package com.example.main

import com.example.AppConfig
import com.example.handler.{DLQHandler, RetryHandler}
import com.example.lineage.LineageTracker
import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.{MinioUtils, OffsetStore, SparkUtils}
import com.example.validation.{QualityThreshold, Validator}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, concat_ws, dayofmonth, lit, month, year}
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object BATCH {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Batch")

    val minioEndpoint = AppConfig.MINIO_ENDPOINT
    val minioAccessKey = AppConfig.MINIO_ACCESS_KEY
    val minioSecretKey = AppConfig.MINIO_SECRET_KEY
    val minioBucketName = AppConfig.MINIO_BUCKET_NAME
    val minioPathStyleAccess = AppConfig.MINIO_PATH_STYLE_ACCESS

    val rawPath = AppConfig.MINIO_BASE_PATH
    val invalidPath = s"${AppConfig.MINIO_BASE_PATH}/batch/invalid"
    val dlqPath = s"${AppConfig.MINIO_BASE_PATH}/batch/dlq"
    val lineagePath = s"${AppConfig.MINIO_BASE_PATH}/lineage"
    val offsetPath = s"${AppConfig.MINIO_BASE_PATH}/batch/offsets"

    val batchId = System.currentTimeMillis().toString
    val topic = AppConfig.KAFKA_BATCH_TOPIC

    // Start async DLQ handler
    DLQHandler.start(spark)

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

      logger.info(s"Reading CDC events from Kafka topic (source: PostgreSQL): $topic")

      val lastOffsets = OffsetStore.getLastOffset(
        spark = spark,
        bucketName = minioBucketName,
        offsetPath = offsetPath,
        topic = topic
      )

      val startingOffsets = if (lastOffsets.nonEmpty) {
        val offsetsJson = lastOffsets.map { case (partition, offset) =>
          s""""$partition":${offset.toLong + 1}"""
        }.mkString("{", ",", "}")
        logger.info(s"Resuming from last processed offsets: $offsetsJson")
        offsetsJson
      } else {
        logger.info("No previous offsets found, starting from earliest")
        "earliest"
      }

      val kafkaOptions = Map(
        "kafka.bootstrap.servers" -> AppConfig.KAFKA_BOOTSTRAP_SERVERS,
        "subscribe" -> AppConfig.KAFKA_BATCH_TOPIC,
        "startingOffsets" -> startingOffsets,
        "kafka.group.id" -> AppConfig.KAFKA_GROUP_ID
      )


      val kafkaDf = spark.read
        .format("kafka")
        .options(kafkaOptions)
        .load()

      val rawRecordCount = kafkaDf.count()
      logger.info(s"Total CDC records read from Kafka: $rawRecordCount")

      if (rawRecordCount == 0) {
        logger.warn("No new records found in Kafka topic. Exiting batch job.")
        DLQHandler.stop()
        spark.stop()
        return
      }

      val parsedDf = Parser.parse(
        kafkaDf,
        Schema.schema,
        AppConfig.SPARK_TIMESTAMP_PATTERN,
        AppConfig.SPARK_TIMEZONE
      )

      val validationResult = Validator.validateAndClean(parsedDf)
      logger.info(s"Validation metrics: total=${validationResult.metrics.totalRecords}, valid=${validationResult.metrics.validRecords}, invalid=${validationResult.metrics.invalidRecords}")

      val qualityConfig = QualityThreshold.getDefaultConfig
      val qualityResult = QualityThreshold.checkThreshold(validationResult.metrics, qualityConfig)

      if (qualityResult.shouldPause) {
        logger.error(s"Pipeline paused due to data quality threshold breach: ${qualityResult.message}")
        DLQHandler.stop()
        spark.stop()
        sys.exit(1)
      }

      if (!validationResult.invalidRecords.isEmpty) {
        DLQHandler.writeToDLQ(
          records = validationResult.invalidRecords,
          path = invalidPath,
          bucketName = minioBucketName,
          batchId = batchId.toLong,
          reason = "invalid_data_quality",
          retryCount = 0
        ) match {
          case Success(_) => logger.info("Invalid records enqueued for async DLQ processing")
          case Failure(ex) => logger.error("Failed to enqueue invalid records to DLQ", ex)
        }
      }

      val validWithPartition = validationResult.validRecords
        .withColumn("year", year(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
        .withColumn(
          "event_key",
          concat_ws("|",
            col("event_time").cast("string"),
            col("user_id").cast("string"),
            col("product_id").cast("string"),
            col("event_type")
          )
        )

      logger.info(s"Valid CDC record count in this batch: ${validWithPartition.count()}")

      val tableExists = scala.util.Try {
        DeltaTable.forPath(spark, s"s3a://$minioBucketName/$rawPath")
      }.isSuccess

      val writeSuccess = if (tableExists) {
        logger.info("Target table exists, using merge for idempotent writes")
        RetryHandler.withRetry(
          {
            MinioUtils.mergeDeltaTable(
              spark = spark,
              bucketName = minioBucketName,
              path = rawPath,
              sourceDataFrame = validWithPartition,
              mergeCondition = "target.event_key = source.event_key",
              updateCondition = None, // Update all matched records
              insertCondition = None  // Insert all non-matched records
            )
          },
          name = "Merge Delta Raw Zone (Idempotent)"
        )
      } else {
        logger.info("Target table does not exist, creating with initial data")
        RetryHandler.withRetry(
          MinioUtils.writeDeltaTable(
            df = validWithPartition,
            bucketName = minioBucketName,
            path = rawPath,
            saveMode = SaveMode.Overwrite,
            partitionColumns = Some(Seq("year", "month", "day")),
            repartitionColumns = Some(Seq("year", "month", "day"))
          ),
          name = "Write Delta Raw Zone (Initial)"
        )
      }

      writeSuccess match {
        case Success(_) =>
          logger.info("Successfully wrote/merged valid records to Delta raw zone")

          OffsetStore.saveOffsetsFromKafka(
            spark = spark,
            bucketName = minioBucketName,
            offsetPath = offsetPath,
            kafkaDf = kafkaDf,
            topic = topic
          ) match {
            case Success(_) => logger.info("Successfully saved Kafka offsets")
            case Failure(ex) => logger.error("Failed to save Kafka offsets", ex)
          }

          LineageTracker.log(
            spark = spark,
            bucketName = minioBucketName,
            lineagePath = lineagePath,
            pipeline = "batch",
            batchId = batchId,
            source = s"Kafka:$topic",
            sink = rawPath,
            metrics = Map(
              "total" -> validationResult.metrics.totalRecords,
              "valid" -> validationResult.metrics.validRecords,
              "invalid" -> validationResult.metrics.invalidRecords,
              "quality_rate" -> (if (validationResult.metrics.totalRecords > 0)
                (validationResult.metrics.validRecords.toDouble / validationResult.metrics.totalRecords.toDouble * 100).toLong
              else 0L)
            ),
            status = "success",
            mode = "ingest"
          )
        case Failure(ex) =>
          logger.error("Failed to write/merge valid records after retries, sending to DLQ", ex)
          DLQHandler.writeToDLQ(
            records = validWithPartition,
            path = dlqPath,
            bucketName = minioBucketName,
            batchId = batchId.toLong,
            reason = s"write_raw_failed: ${ex.getMessage}",
            retryCount = 0
          ) match {
            case Success(_) => logger.info("Failed records enqueued for async DLQ processing")
            case Failure(dlqEx) => logger.error("CRITICAL: Failed to enqueue to DLQ", dlqEx)
          }

          LineageTracker.log(
            spark = spark,
            bucketName = minioBucketName,
            lineagePath = lineagePath,
            pipeline = "batch",
            batchId = batchId,
            source = s"Kafka:$topic",
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

      logger.info("Waiting for async DLQ handler to finish...")
      Thread.sleep(5000)

    } catch {
      case NonFatal(ex) =>
        logger.error("Batch job failed", ex)
        throw ex
    } finally {
      DLQHandler.stop()
      spark.stop()
    }
  }
}