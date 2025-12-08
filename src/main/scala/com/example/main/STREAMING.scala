package com.example.main

import com.example.AppConfig
import com.example.handler.{BackpressureHandler, DLQHandler, RetryHandler}
import com.example.lineage.LineageTracker
import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.{ClickHouseUtils, DeduplicationStore, MinioUtils, SparkUtils}
import com.example.validation.{QualityThreshold, Validator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object STREAMING {
  private val logger = LoggerFactory.getLogger(getClass)

  val kafkaBootstrapServer: String = AppConfig.KAFKA_BOOTSTRAP_SERVERS
  val kafkaTopic: String = AppConfig.KAFKA_STREAM_TOPIC
  val kafkaGroupId: String = AppConfig.KAFKA_GROUP_ID
  val kafkaStartingOffsets: String = AppConfig.KAFKA_STARTING_OFFSETS

  val clickhouseUrl: String = AppConfig.CLICKHOUSE_URL
  val clickhouseUser: String = AppConfig.CLICKHOUSE_USER
  val clickhousePassword: String = AppConfig.CLICKHOUSE_PASSWORD
  val clickhouseTable: String = AppConfig.CLICKHOUSE_TABLE

  val invalidPath: String = s"${AppConfig.MINIO_BASE_PATH}/streaming/invalid"
  val dlqPath: String = s"${AppConfig.MINIO_BASE_PATH}/streaming/dlq"
  val lineagePath: String = s"${AppConfig.MINIO_BASE_PATH}/streaming/lineage"
  val dedupPath: String = s"${AppConfig.MINIO_BASE_PATH}/streaming/deduplication"

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Streaming")

    RetryHandler.withRetry(
      MinioUtils.configureMinIO(
        spark,
        AppConfig.MINIO_ENDPOINT,
        AppConfig.MINIO_ACCESS_KEY,
        AppConfig.MINIO_SECRET_KEY,
        AppConfig.MINIO_PATH_STYLE_ACCESS
      ),
      name = "MinIO Configuration"
    ) match {
      case Success(_) => logger.info("MinIO configured successfully")
      case Failure(exception) =>
        logger.error("Failed to configure MinIO after retries", exception)
        sys.exit(1)
    }

    ClickHouseUtils.initialize(
      url = clickhouseUrl,
      user = clickhouseUser,
      password = clickhousePassword,
      batchSize = AppConfig.CLICKHOUSE_BATCH_SIZE,
      maxConnections = sys.env.get("CLICKHOUSE_MAX_CONNECTIONS").flatMap(v => Try(v.toInt).toOption).getOrElse(10)
    ) match {
      case Success(_) => logger.info("ClickHouse connection pool initialized")
      case Failure(exception) =>
        logger.error("Failed to initialize ClickHouse connection pool", exception)
        sys.exit(1)
    }

    DeduplicationStore.initialize(
      spark = spark,
      bucketName = AppConfig.MINIO_BUCKET_NAME,
      dedupPath = dedupPath,
      maxCacheSize = sys.env.get("DEDUP_CACHE_SIZE").flatMap(v => Try(v.toInt).toOption).getOrElse(1000000)) match {
      case Success(_) => logger.info("Deduplication store initialized")
      case Failure(exception) =>
        logger.warn("Failed to initialize deduplication store, continuing without persistence", exception)
    }

    DLQHandler.start(spark)

    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", kafkaStartingOffsets)
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", sys.env.get("KAFKA_MAX_OFFSETS_PER_TRIGGER")
        .flatMap(v => Try(v.toLong).toOption).getOrElse(100000L))
      .load()

    val parsedStream = Parser.parse(
      kafkaDf,
      Schema.schema,
      AppConfig.SPARK_TIMESTAMP_PATTERN,
      AppConfig.SPARK_TIMEZONE
    ).withWatermark("event_time", AppConfig.SPARK_WATERMARK_DURATION)

    val checkpointLocation = AppConfig.KAFKA_CHECKPOINT_LOCATION

    val query: StreamingQuery = parsedStream.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (batchDF.isEmpty) {
          logger.debug(s"[batch $batchId] Skipping empty micro-batch.")
        } else {
          logger.info(s"[batch $batchId] Processing micro-batch with ${batchDF.count()} records")

          val deduplicatedDF = DeduplicationStore.filterDuplicates(
            batchDF,
            keyColumns = Seq("event_time", "user_id", "product_id", "event_type")
          )

          val dedupCount = batchDF.count() - deduplicatedDF.count()
          if (dedupCount > 0) {
            logger.info(s"[batch $batchId] Filtered $dedupCount duplicate records")
          }

          val validationResult = Validator.validateAndClean(deduplicatedDF)
          logger.info(s"[batch $batchId] Validation metrics: total=${validationResult.metrics.totalRecords}, valid=${validationResult.metrics.validRecords}, invalid=${validationResult.metrics.invalidRecords}")

          val qualityConfig = QualityThreshold.getDefaultConfig
          val qualityResult = QualityThreshold.checkThreshold(validationResult.metrics, qualityConfig)

          if (qualityResult.shouldPause) {
            logger.error(s"[batch $batchId] Pipeline should pause due to quality threshold breach: ${qualityResult.message}")
          }

          // Save invalid records asynchronously (non-blocking)
          if (!validationResult.invalidRecords.isEmpty) {
            DLQHandler.writeToDLQ(
              records = validationResult.invalidRecords,
              path = invalidPath,
              bucketName = AppConfig.MINIO_BUCKET_NAME,
              batchId = batchId,
              reason = "invalid_data_quality",
              retryCount = 0
            ) match {
              case Success(_) => logger.debug(s"[batch $batchId] Invalid records enqueued for async DLQ processing")
              case Failure(ex) => logger.error(s"[batch $batchId] Failed to enqueue invalid records to DLQ", ex)
            }
          }

          RetryHandler.withRetry(
            {
              val count = validationResult.validRecords.count()
              if (count > 0) {
                logger.info(s"[batch $batchId] Writing $count valid records to ClickHouse table $clickhouseTable")
                validationResult.validRecords.write
                  .mode("append")
                  .jdbc(clickhouseUrl, clickhouseTable, ClickHouseUtils.getConnectionProperties)
              } else {
                logger.info(s"[batch $batchId] No valid records to write")
              }
            },
            name = s"ClickHouse write (batch $batchId)"
          ) match {
            case Success(_) =>
              logger.info(s"[batch $batchId] ClickHouse write succeeded")

              if (batchId % 10 == 0) {
                DeduplicationStore.persistToDelta(spark, AppConfig.MINIO_BUCKET_NAME, dedupPath) match {
                  case Success(_) => logger.debug(s"[batch $batchId] Deduplication keys persisted")
                  case Failure(ex) => logger.warn(s"[batch $batchId] Failed to persist deduplication keys", ex)
                }
              }

              LineageTracker.log(
                spark = spark,
                bucketName = AppConfig.MINIO_BUCKET_NAME,
                lineagePath = lineagePath,
                pipeline = "streaming",
                batchId = batchId.toString,
                source = s"Kafka:${AppConfig.KAFKA_STREAM_TOPIC}",
                sink = s"ClickHouse:$clickhouseTable",
                metrics = Map(
                  "total" -> validationResult.metrics.totalRecords,
                  "valid" -> validationResult.metrics.validRecords,
                  "invalid" -> validationResult.metrics.invalidRecords,
                  "duplicates_filtered" -> dedupCount,
                  "quality_rate" -> (if (validationResult.metrics.totalRecords > 0)
                    (validationResult.metrics.validRecords.toDouble / validationResult.metrics.totalRecords.toDouble * 100).toLong
                  else 0L)
                ),
                status = "success",
                mode = "stream"
              )
            case Failure(ex) =>
              logger.error(s"[batch $batchId] ClickHouse write failed after retries, sending to DLQ", ex)
              DLQHandler.writeToDLQ(
                records = validationResult.validRecords,
                path = dlqPath,
                bucketName = AppConfig.MINIO_BUCKET_NAME,
                batchId = batchId,
                reason = s"clickhouse_write_failed: ${ex.getMessage}",
                retryCount = 0
              ) match {
                case Success(_) => logger.info(s"[batch $batchId] Failed records enqueued for async DLQ processing")
                case Failure(dlqEx) => logger.error(s"[batch $batchId] CRITICAL: Failed to enqueue to DLQ", dlqEx)
              }

              LineageTracker.log(
                spark = spark,
                bucketName = AppConfig.MINIO_BUCKET_NAME,
                lineagePath = lineagePath,
                pipeline = "streaming",
                batchId = batchId.toString,
                source = s"Kafka:${AppConfig.KAFKA_STREAM_TOPIC}",
                sink = s"ClickHouse:$clickhouseTable",
                metrics = Map(
                  "total" -> validationResult.metrics.totalRecords,
                  "valid" -> validationResult.metrics.validRecords,
                  "invalid" -> validationResult.metrics.invalidRecords
                ),
                status = "failed",
                mode = "stream",
                message = s"clickhouse_write_failed: ${ex.getMessage}"
              )
          }
        }
      }
      .start()

    val backpressureConfig = BackpressureHandler.getDefaultConfig
    val monitorThread = BackpressureHandler.monitorBackpressure(
      query = query,
      config = backpressureConfig,
      onBackpressure = { result =>
        if (result.shouldPause) {
          logger.error(s"Stopping stream due to backpressure: ${result.message}")
          query.stop()
        } else {
          logger.warn(s"Backpressure detected: ${result.message}")
        }
      }
    )

    try {
      query.awaitTermination()
    } finally {
      logger.info("Shutting down streaming pipeline...")
      monitorThread.interrupt()
      DLQHandler.stop()
      DeduplicationStore.persistToDelta(spark, AppConfig.MINIO_BUCKET_NAME, dedupPath) match {
        case Success(_) => logger.info("Final deduplication keys persisted")
        case Failure(ex) => logger.warn("Failed to persist final deduplication keys", ex)
      }
      ClickHouseUtils.close()
      spark.stop()
    }
  }
}