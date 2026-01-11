package com.example.main.streaming

import com.example.config.AppConfig
import com.example.handler.RetryHandler
import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.{ClickHouseUtils, GcsUtils, SparkUtils}
import com.example.validation.Validator
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object STREAMING {
  private val logger = LoggerFactory.getLogger(getClass)

  private val DLQ_TOPIC = sys.env.getOrElse("KAFKA_DLQ_TOPIC", "ecommerce_dlq")

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Streaming")

    val dedupKeyColumns = AppConfig.applicationConfig.pipeline.dedupKeyColumns

    val gcsSettings = AppConfig.applicationConfig.gcs.getOrElse(
      throw new IllegalStateException("GCS configuration is required. Please configure 'gcs' block in application.conf")
    )

    RetryHandler.withRetry(
      GcsUtils.configureGcs(
        spark,
        gcsSettings.projectId,
        gcsSettings.credentialsPath
      ),
      name = "GCS Configuration"
    ) match {
      case Success(_) => logger.info("GCS configured successfully")
      case Failure(exception) =>
        logger.error("Failed to configure GCS", exception)
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
      .dropDuplicates(dedupKeyColumns)

    val query: StreamingQuery = parsedStream.writeStream
      .outputMode("append")
      .option("checkpointLocation", s"${AppConfig.KAFKA_CHECKPOINT_LOCATION}/streaming")
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
              writeToKafkaDLQ(validationResult.invalidRecords, batchId, "validation_failed")
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
                  logger.info(s"[batch $batchId] ClickHouse write succeeded")
                case Failure(ex) =>
                  logger.error(s"[batch $batchId] ClickHouse write failed, sending to DLQ", ex)
                  writeToKafkaDLQ(validationResult.validRecords, batchId, s"clickhouse_write_failed: ${ex.getMessage}")
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
      ClickHouseUtils.close()
      spark.stop()
    }
  }

  private def writeToKafkaDLQ(df: DataFrame, batchId: Long, reason: String): Unit = {
    Try {
      val dlqDf = df
        .withColumn("dlq_batch_id", lit(batchId))
        .withColumn("dlq_reason", lit(reason))
        .withColumn("dlq_timestamp", current_timestamp())
        .select(
          to_json(struct(df.columns.map(col): _*)).alias("value")
        )

      dlqDf.write
        .format("kafka")
        .option("kafka.bootstrap.servers", AppConfig.KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", DLQ_TOPIC)
        .save()

      logger.info(s"[batch $batchId] Records sent to Kafka DLQ topic: $DLQ_TOPIC")
    } match {
      case Success(_) => // logged above
      case Failure(ex) =>
        logger.error(s"[batch $batchId] CRITICAL: Failed to write to Kafka DLQ", ex)
    }
  }
}