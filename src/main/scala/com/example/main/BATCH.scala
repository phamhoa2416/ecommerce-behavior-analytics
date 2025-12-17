package com.example.main

import com.example.config.AppConfig
import com.example.handler.{DLQHandler, RetryHandler}
import com.example.lineage.LineageTracker
import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.{MinioUtils, SparkUtils}
import com.example.validation.Validator
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, dayofmonth, lit, max, month, year}
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object BATCH {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Batch")

    val minioBucketName = AppConfig.MINIO_BUCKET_NAME

    val rawPath = AppConfig.MINIO_BASE_PATH
    val invalidPath = AppConfig.PIPELINE_BATCH_INVALID_PATH
    val dlqPath = AppConfig.PIPELINE_BATCH_DLQ_PATH
    val lineagePath = AppConfig.PIPELINE_BATCH_LINEAGE_PATH

    val batchId = System.currentTimeMillis().toString
    val topic = AppConfig.KAFKA_BATCH_TOPIC

    DLQHandler.start(spark)

    try {
      RetryHandler.withRetry(
        MinioUtils.configureMinIO(
          spark,
          AppConfig.MINIO_ENDPOINT,
          AppConfig.MINIO_ACCESS_KEY,
          AppConfig.MINIO_SECRET_KEY,
          AppConfig.MINIO_PATH_STYLE_ACCESS),
        name = "MinIO Configuration"
      ) match {
        case Success(_) => logger.info("MinIO configured successfully")
        case Failure(exception) =>
          logger.error("Failed to configure MinIO", exception)
          sys.exit(1)
      }

      RetryHandler.withRetry(
        MinioUtils.checkBucketExists(
          AppConfig.MINIO_ENDPOINT,
          AppConfig.MINIO_ACCESS_KEY,
          AppConfig.MINIO_SECRET_KEY, minioBucketName),
        name = "MinIO Bucket Check/Create"
      ) match {
        case Success(_) => logger.info(s"MinIO bucket '$minioBucketName' is ready")
        case Failure(exception) =>
          logger.error(s"Error while checking/creating MinIO bucket '$minioBucketName'", exception)
          sys.exit(1)
      }

      val lastProcessedTimestamp = getLastProcessedTimestamp(spark, minioBucketName, rawPath)
      logger.info(s"Processing records after watermark: $lastProcessedTimestamp ms")


      val kafkaDf = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", AppConfig.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
//        .filter(col("timestamp") > lit(lastProcessedTimestamp))

      val recordCount = kafkaDf.count()
      logger.info(s"Records to process: $recordCount")

      if (recordCount == 0) {
        logger.warn("No new records found in Kafka topic. Exiting batch job.")
        DLQHandler.stop()
        spark.stop()
        return
      }

      val parsedDf = Parser.parseCDC(
        kafkaDf,
        Schema.debeziumCDC,
        AppConfig.SPARK_TIMESTAMP_PATTERN,
        AppConfig.SPARK_TIMEZONE
      )

      val validationResult = Validator.validateAndClean(parsedDf)
      logger.info(s"Validation: total=${validationResult.metrics.totalRecords}, " +
        s"valid=${validationResult.metrics.validRecords}, " +
        s"invalid=${validationResult.metrics.invalidRecords}")

      // Write invalid records to DLQ
      if (validationResult.metrics.invalidRecords > 0) {
        DLQHandler.writeToDLQ(
          records = validationResult.invalidRecords,
          path = invalidPath,
          bucketName = minioBucketName,
          batchId = batchId.toLong,
          reason = "invalid_data_quality",
          retryCount = 0
        ) match {
          case Success(_) => logger.info("Invalid records sent to DLQ")
          case Failure(ex) => logger.error("Failed to write to DLQ", ex)
        }
      }

      if (validationResult.metrics.validRecords == 0) {
        logger.info("No valid records to process. Batch completed.")

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
          status = "success",
          mode = "ingest",
          message = "No valid records"
        )

        DLQHandler.stop()
        spark.stop()
        return
      }

      val dedupKeyColumns = AppConfig.applicationConfig.pipeline.dedupKeyColumns

      val preparedDf = validationResult.validRecords
        .withColumn("year", year(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
        .withColumn(
          "event_key",
          concat_ws("|", dedupKeyColumns.map(c => col(c)): _*)
        )

      logger.info(s"Writing ${validationResult.metrics.validRecords} valid records to Delta")

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
              sourceDataFrame = preparedDf,
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
            df = preparedDf,
            bucketName = minioBucketName,
            path = rawPath,
            saveMode = SaveMode.Overwrite,
            partitionColumns = Some(Seq("year", "month", "day")),
            repartitionColumns = Some(Seq("year", "month", "day"))
          ),
          name = "Delta Write"
        )
      }

      writeSuccess match {
        case Success(_) =>
          logger.info("Successfully wrote to Delta table")

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
              "watermark" -> lastProcessedTimestamp
            ),
            status = "success",
            mode = "ingest"
          )
        case Failure(ex) =>
          logger.error("Failed to write/merge valid records after retries, sending to DLQ", ex)
          DLQHandler.writeToDLQ(
            records = preparedDf,
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
            message = s"delta_write_failed: ${ex.getMessage}"
          )
      }


    } catch {
      case NonFatal(ex) =>
        logger.error("Batch job failed", ex)
        throw ex
    } finally {
      DLQHandler.stop()
      spark.stop()
    }
  }

  private def getLastProcessedTimestamp(
                                         spark: SparkSession,
                                         bucketName: String,
                                         path: String
                                       ): Long = {
    try {
      val deltaTable = DeltaTable.forPath(spark, s"s3a://$bucketName/$path")
      val maxTimestamp = deltaTable.toDF
        .agg(max("event_time").as("max_time"))
        .collect()
        .headOption
        .flatMap(row => Option(row.getTimestamp(0)))
        .map(_.getTime)
        .getOrElse(0L)

      logger.info(s"Found watermark: $maxTimestamp ms")
      maxTimestamp
    } catch {
      case _: Exception =>
        logger.info("No existing table, starting from 0")
        0L
    }
  }
}