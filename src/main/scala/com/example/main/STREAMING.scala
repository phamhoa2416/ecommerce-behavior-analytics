package com.example.main

import com.example.config.AppConfig
import com.example.handler.{DLQHandler, RetryHandler}
import com.example.lineage.LineageTracker
import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.{ClickHouseUtils, MinioUtils, SparkUtils}
import com.example.validation.Validator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object STREAMING {
  private val logger = LoggerFactory.getLogger(getClass)

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
        logger.error("Failed to configure MinIO", exception)
        sys.exit(1)
    }

    ClickHouseUtils.initialize(
      url = AppConfig.CLICKHOUSE_URL,
      user = AppConfig.CLICKHOUSE_USER,
      password = AppConfig.CLICKHOUSE_PASSWORD,
      batchSize = AppConfig.CLICKHOUSE_BATCH_SIZE,
      maxConnections = AppConfig.clickhouseSettings.maxConnections
    ) match {
      case Success(_) => logger.info("ClickHouse initialized")
      case Failure(exception) =>
        logger.error("Failed to initialize ClickHouse", exception)
        sys.exit(1)
    }

    DLQHandler.start(spark)

    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", AppConfig.KAFKA_STREAM_TOPIC)
      .option("startingOffsets", AppConfig.KAFKA_STARTING_OFFSETS)
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
      .dropDuplicates(Seq("event_time", "user_id", "product_id", "event_type"))

    val query: StreamingQuery = parsedStream.writeStream
      .outputMode("append")
      .option("checkpointLocation", AppConfig.KAFKA_CHECKPOINT_LOCATION)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (batchDF.isEmpty) {
          logger.debug(s"[batch $batchId] Empty batch, skipping")
        } else {
          val cachedDf = batchDF.persist()
          val batchCount = cachedDf.count()
          logger.info(s"[batch $batchId] Processing $batchCount records")

          try {
            val validationResult = Validator.validateAndClean(cachedDf)
            logger.info(s"[batch $batchId] Valid: ${validationResult.metrics.validRecords}, " +
              s"Invalid: ${validationResult.metrics.invalidRecords}")

            if (validationResult.metrics.invalidRecords > 0) {
              DLQHandler.writeToDLQ(
                records = validationResult.invalidRecords,
                path = AppConfig.PIPELINE_STREAMING_INVALID_PATH,
                bucketName = AppConfig.MINIO_BUCKET_NAME,
                batchId = batchId,
                reason = "validation_failed",
                retryCount = 0
              ) match {
                case Success(_) => logger.debug(s"[batch $batchId] Invalid records sent to DLQ")
                case Failure(ex) => logger.warn(s"[batch $batchId] Failed to write to DLQ: ${ex.getMessage}")
              }
            }

            val validCount = validationResult.metrics.validRecords
            if (validCount > 0) {
              RetryHandler.withRetry(
                {
                  logger.info(s"[batch $batchId] Writing $validCount records to ClickHouse")
                  validationResult.validRecords.write
                    .mode("append")
                    .jdbc(
                      AppConfig.CLICKHOUSE_URL,
                      AppConfig.CLICKHOUSE_TABLE,
                      ClickHouseUtils.getConnectionProperties
                    )
                },
                name = s"ClickHouse write (batch $batchId)"
              ) match {
                case Success(_) =>
                  logger.info(s"[batch $batchId] Write succeeded")

                  LineageTracker.log(
                    spark = spark,
                    bucketName = AppConfig.MINIO_BUCKET_NAME,
                    lineagePath = AppConfig.PIPELINE_STREAMING_LINEAGE_PATH,
                    pipeline = "streaming",
                    batchId = batchId.toString,
                    source = s"Kafka:${AppConfig.KAFKA_STREAM_TOPIC}",
                    sink = s"ClickHouse:${AppConfig.CLICKHOUSE_TABLE}",
                    metrics = Map(
                      "total" -> validationResult.metrics.totalRecords,
                      "valid" -> validationResult.metrics.validRecords,
                      "invalid" -> validationResult.metrics.invalidRecords
                    ),
                    status = "success",
                    mode = "stream"
                  )
                case Failure(ex) =>
                  logger.error(s"[batch $batchId] Write failed, sending to DLQ", ex)

                  DLQHandler.writeToDLQ(
                    records = validationResult.validRecords,
                    path = AppConfig.PIPELINE_STREAMING_DLQ_PATH,
                    bucketName = AppConfig.MINIO_BUCKET_NAME,
                    batchId = batchId,
                    reason = s"clickhouse_write_failed: ${ex.getMessage}",
                    retryCount = 0
                  ) match {
                    case Success(_) => logger.info(s"[batch $batchId] Failed records sent to DLQ")
                    case Failure(dlqEx) => logger.error(s"[batch $batchId] CRITICAL: DLQ write failed", dlqEx)
                  }

                  LineageTracker.log(
                    spark = spark,
                    bucketName = AppConfig.MINIO_BUCKET_NAME,
                    lineagePath = AppConfig.PIPELINE_STREAMING_LINEAGE_PATH,
                    pipeline = "streaming",
                    batchId = batchId.toString,
                    source = s"Kafka:${AppConfig.KAFKA_STREAM_TOPIC}",
                    sink = s"ClickHouse:${AppConfig.CLICKHOUSE_TABLE}",
                    metrics = Map(
                      "total" -> validationResult.metrics.totalRecords,
                      "valid" -> validationResult.metrics.validRecords,
                      "invalid" -> validationResult.metrics.invalidRecords
                    ),
                    status = "failed",
                    mode = "stream",
                    message = ex.getMessage
                  )
              }
            } else {
              logger.info(s"[batch $batchId] No valid records to write")
            }

          } finally {
            cachedDf.unpersist()
          }
        }
      }
      .start()

    try {
      query.awaitTermination()
    } finally {
      logger.info("Shutting down streaming pipeline")
      DLQHandler.stop()
      ClickHouseUtils.close()
      spark.stop()
    }
  }
}