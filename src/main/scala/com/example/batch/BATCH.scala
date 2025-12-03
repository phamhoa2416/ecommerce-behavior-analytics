package com.example.batch

import com.example.AppConfig
import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.{MinioUtils, SparkUtils}
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

    try {
      MinioUtils.configureMinIO(spark, minioEndpoint, minioAccessKey, minioSecretKey, minioPathStyleAccess)
      MinioUtils.checkBucketExists(minioEndpoint, minioAccessKey, minioSecretKey, minioBucketName) match {
        case Success(_) => logger.info(s"MinIO bucket '$minioBucketName' is ready.")
        case Failure(exception) =>
          logger.error(s"Error while checking/creating MinIO bucket '$minioBucketName'", exception)
          sys.exit(1)
      }

      val kafkaDf = spark.read
        .format("kafka")
        .options(kafkaOptions)
        .load()

      logger.info(s"Reading CDC events from Kafka topic (source: PostgreSQL): ${AppConfig.KAFKA_BATCH_TOPIC}")

      val parsedEvents = Parser.parseToEcommerceEvents(
        kafkaDf,
        Schema.schema,
        AppConfig.SPARK_TIMESTAMP_PATTERN,
        AppConfig.SPARK_TIMEZONE
      )

      val dataWithPartition = parsedEvents.toDF()
        .withColumn("year", year(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))

      logger.info(s"Parsed CDC record count in this batch: ${dataWithPartition.count()}")

      // Raw zone: append-only to preserve full history of all CDC events
      // Each CDC event (INSERT/UPDATE/DELETE) is stored as a separate record
      // This allows downstream processing to reconstruct state at any point in time
      val deltaPath = s"s3a://$minioBucketName/ecommerce_events"
      logger.info(s"Appending raw CDC data to Delta Lake (raw zone) on MinIO: $deltaPath")

      dataWithPartition
        .repartition(col("year"), col("month"), col("day"))
        .write
        .format("delta")
        .mode(SaveMode.Append)
        .partitionBy("year", "month", "day")
        .save(deltaPath)

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

