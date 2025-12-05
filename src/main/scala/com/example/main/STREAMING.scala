package com.example.main

import com.example.AppConfig
import com.example.handler.{DeadLetterQueueHandler, RetryHandler}
import com.example.lineage.LineageTracker
import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.SparkUtils
import com.example.validation.Validator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.slf4j.LoggerFactory

import java.util.Properties
import scala.util.{Failure, Success}

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
  val lineagePath: String = s"${AppConfig.MINIO_BASE_PATH}/lineage"

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Streaming")

    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", kafkaStartingOffsets)
      .option("failOnDataLoss", "false")
      .load()

    val parsedStream = Parser.parse(
      kafkaDf,
      Schema.schema,
      AppConfig.SPARK_TIMESTAMP_PATTERN,
      AppConfig.SPARK_TIMEZONE
    ).withWatermark("event_time", "5 minutes")
      .dropDuplicates("event_time", "user_id", "product_id", "event_type")

    val checkpointLocation = AppConfig.KAFKA_CHECKPOINT_LOCATION

    val connection = new Properties()
    connection.put("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    connection.put("user", clickhouseUser)
    connection.put("password", clickhousePassword)
    connection.put("batchsize", AppConfig.CLICKHOUSE_BATCH_SIZE.toString)

    val query: StreamingQuery = parsedStream.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (batchDF.isEmpty) {
          logger.debug("Skipping empty micro-batch.")
        } else {
          logger.info(s"Processing micro-batch $batchId with ${batchDF.count()} records")

          val validationResult = Validator.validateAndClean(batchDF)
          logger.info(s"[batch $batchId] Validation metrics: total=${validationResult.metrics.totalRecords}, valid=${validationResult.metrics.validRecords}, invalid=${validationResult.metrics.invalidRecords}")

          Validator.saveInvalidRecords(
            invalidRecords = validationResult.invalidRecords,
            bucketName = AppConfig.MINIO_BUCKET_NAME,
            path = invalidPath
          )

          RetryHandler.withRetry(
            {
              val count = validationResult.validRecords.count()
              if (count > 0) {
                logger.info(s"[batch $batchId] Writing $count valid records to ClickHouse table $clickhouseTable")
                validationResult.validRecords.write
                  .mode("append")
                  .jdbc(clickhouseUrl, clickhouseTable, connection)
              } else {
                logger.info(s"[batch $batchId] No valid records to write")
              }
            },
            name = s"ClickHouse write (batch $batchId)"
          ) match {
            case Success(_) =>
              logger.info(s"[batch $batchId] ClickHouse write succeeded")
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
                status = "success",
                mode = "stream"
              )
            case Failure(ex) =>
              logger.error(s"[batch $batchId] ClickHouse write failed after retries, sending to DLQ", ex)
              DeadLetterQueueHandler.writeToDLQ(
                records = validationResult.validRecords,
                path = dlqPath,
                bucketName = AppConfig.MINIO_BUCKET_NAME,
                batchId = batchId,
                reason = s"clickhouse_write_failed: ${ex.getMessage}",
                retryCount = 0
              )
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

    query.awaitTermination()
  }
}

