package com.example.main.batch

import com.example.config.AppConfig
import com.example.handler.RetryHandler
import com.example.util.{GcsUtils, SparkUtils}
import com.example.validation.Validator
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

import java.time.LocalDate
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object WORKING_ZONE {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Working Zone")

    val gcsSettings = AppConfig.applicationConfig.gcs.getOrElse(
      throw new IllegalStateException("GCS configuration is required.")
    )
    val gcsBucketName = gcsSettings.bucketName
    val rawPath = s"${gcsSettings.basePath}/raw_zone"
    val workingPath = s"${gcsSettings.basePath}/working_zone"
    val dedupKeyColumns = AppConfig.applicationConfig.pipeline.dedupKeyColumns

    val processingDate = if (args.nonEmpty) args(0) else LocalDate.now().toString

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

      logger.info(s"Reading Raw Zone for ingestion_date = $processingDate")
      val rawDf = Try(
        spark.read.format("delta")
          .load(s"gs://$gcsBucketName/$rawPath")
          .filter(col("ingestion_date") === lit(processingDate))
      ) match {
        case Success(df) =>
          logger.info("Successfully read Raw Zone data")
          df
        case Failure(ex) =>
          logger.error("Failed to read Raw Zone data", ex)
          throw ex
      }

      val cachedRawDf = rawDf.cache()
      val recordCount = cachedRawDf.count()

      if (recordCount == 0) {
        logger.warn(s"No records found in Raw Zone for ingestion_date=$processingDate. Exiting.")
        cachedRawDf.unpersist()
        spark.stop()
        return
      }

      logger.info(s"Read $recordCount records from Raw Zone")

      val parsedDf = cachedRawDf.select(
          col("data.payload.event_time").alias("event_time_raw"),
          col("data.payload.event_type"),
          col("data.payload.product_id"),
          col("data.payload.category_id"),
          col("data.payload.category_code"),
          col("data.payload.brand"),
          col("data.payload.price").alias("price_raw"),
          col("data.payload.user_id"),
          col("data.payload.user_session"),
          col("topic"),
          col("partition").alias("kafka_partition"),
          col("offset").alias("kafka_offset"),
          col("kafka_timestamp"),
          col("ingestion_ts"),
          col("ingestion_date")
        )
        .withColumn("event_time", timestamp_micros(col("event_time_raw")))
        .drop("event_time_raw")
        .withColumn(
          "price",
          (conv(hex(col("price_raw")), 16, 10).cast("int") / 100.0).cast("decimal(10, 2)")
        )
        .drop("price_raw")

      cachedRawDf.unpersist()

      val validationResult = Validator.validateAndClean(parsedDf)
      logger.info(s"Validation: total=${validationResult.metrics.totalRecords}, " +
        s"valid=${validationResult.metrics.validRecords}, " +
        s"invalid=${validationResult.metrics.invalidRecords}")

      if (validationResult.metrics.validRecords == 0) {
        logger.info("No valid records to process. Working Zone batch completed.")
        spark.stop()
        return
      }

      val preparedDf = validationResult.validRecords
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("event_key", concat_ws("|", dedupKeyColumns.map(c => col(c)): _*))

      val tableExists = Try(DeltaTable.forPath(spark, s"gs://$gcsBucketName/$workingPath")).isSuccess

      val writeResult = if (tableExists) {
        logger.info("Working Zone table exists, using merge for CDC deduplication")
        val deltaTable = DeltaTable.forPath(spark, s"gs://$gcsBucketName/$workingPath")

        RetryHandler.withRetry(
          Try {
            deltaTable.as("target")
              .merge(
                preparedDf.as("source"),
                "target.event_key = source.event_key"
              )
              .whenMatched("source.event_time >= target.event_time")
              .updateAll()
              .whenNotMatched()
              .insertAll()
              .execute()
          },
          name = "Working Zone Delta Merge"
        )
      } else {
        logger.info("Working Zone table does not exist, creating with initial data")
        RetryHandler.withRetry(
          GcsUtils.writeDeltaTable(
            df = preparedDf,
            bucketName = gcsBucketName,
            path = workingPath,
            saveMode = SaveMode.Overwrite,
            partitionColumns = Some(Seq("event_date")),
            repartitionColumns = Some(Seq("event_date"))
          ),
          name = "Working Zone Delta Write"
        )
      }

      writeResult match {
        case Success(_) =>
          logger.info("Working Zone batch write completed successfully")
        case Failure(ex) =>
          logger.error("Failed to write Working Zone", ex)
          throw ex
      }

    } catch {
      case NonFatal(ex) =>
        logger.error("Working Zone batch job failed", ex)
        throw ex
    } finally {
      spark.stop()
    }
  }
}
