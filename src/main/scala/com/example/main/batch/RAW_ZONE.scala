package com.example.main.batch

import com.example.config.AppConfig
import com.example.handler.RetryHandler
import com.example.schema.Schema
import com.example.util.{GcsUtils, SparkUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object RAW_ZONE {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Raw Zone")

    val gcsSettings = AppConfig.applicationConfig.gcs.getOrElse(
      throw new IllegalStateException("GCS configuration is required")
    )
    val gcsBucketName = gcsSettings.bucketName
    val rawPath = s"${gcsSettings.basePath}/raw_zone"
    val topic = AppConfig.KAFKA_BATCH_TOPIC
    val checkpointPath = AppConfig.KAFKA_CHECKPOINT_LOCATION

    try {
      RetryHandler.withRetry(
        GcsUtils.configureGcs(
          spark,
          gcsSettings.projectId,
          gcsSettings.credentialsPath),
        name = "GCS Configuration"
      ) match {
        case Success(_) => logger.info("GCS configured successfully")
        case Failure(exception) =>
          logger.error("Failed to configure GCS", exception)
          sys.exit(1)
      }

      RetryHandler.withRetry(
        GcsUtils.checkBucketExists(
          gcsSettings.projectId,
          gcsBucketName,
          gcsSettings.credentialsPath),
        name = "GCS Bucket Check/Create"
      ) match {
        case Success(_) => logger.info(s"GCS bucket '$gcsBucketName' is ready")
        case Failure(exception) =>
          logger.error(s"Error while checking/creating GCS bucket '$gcsBucketName'", exception)
          sys.exit(1)
      }

      val kafkaOptions = AppConfig.getKafkaOptions
      val kafkaDf = spark.readStream
        .format("kafka")
        .options(kafkaOptions)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 100000)
        .load()

      val rawDf = kafkaDf.select(
          col("value").cast("string").alias("raw_payload"),
          from_json(col("value").cast("string"), Schema.debeziumCDC).alias("data"),
          col("topic"),
          col("partition"),
          col("offset"),
          col("timestamp").alias("kafka_timestamp")
        )
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("ingestion_date", to_date(col("ingestion_ts")))
        .filter(col("data.op").isNotNull && col("data.op") =!= "d")

      val query = rawDf.writeStream
        .trigger(Trigger.Once())
        .option("checkpointLocation", s"$checkpointPath/raw_zone")
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          if (!batchDf.isEmpty) {
            logger.info(s"Processing batch $batchId")
            writeToRawZone(batchDf, gcsBucketName, rawPath) match {
              case Success(_) =>
                logger.info(s"Batch $batchId: Raw Zone write completed successfully")
              case Failure(ex) =>
                logger.error(s"Batch $batchId: Failed to write Raw Zone", ex)
                throw ex
            }
          } else {
            logger.info(s"Batch $batchId: No records to process")
          }
        }
        .start()

      query.awaitTermination()
      logger.info("Raw Zone batch job completed successfully")

    } catch {
      case NonFatal(ex) =>
        logger.error("Raw Zone batch job failed", ex)
        throw ex
    } finally {
      spark.stop()
    }
  }

  private def writeToRawZone(
                              df: DataFrame,
                              bucket: String,
                              path: String
                            ): scala.util.Try[Unit] = {
    logger.info("Writing CDC raw data to GCS")

    RetryHandler.withRetry(
      GcsUtils.writeDeltaTable(
        df = df,
        bucketName = bucket,
        path = path,
        saveMode = SaveMode.Append,
        partitionColumns = Some(Seq("ingestion_date"))
      ),
      name = "Raw Zone Delta Append"
    )
  }
}
