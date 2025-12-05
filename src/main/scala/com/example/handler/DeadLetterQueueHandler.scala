package com.example.handler

import com.example.util.MinioUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, dayofmonth, lit, month, year}
import org.slf4j.LoggerFactory


object DeadLetterQueueHandler {
  private val logger = LoggerFactory.getLogger(getClass)

  def writeToDLQ(
                  records: DataFrame,
                  path: String,
                  bucketName: String,
                  batchId: Long,
                  reason: String,
                  retryCount: Int
                ): Unit = {
    try {
      import records.sparkSession.implicits._

      val dlqRecords = records
        .select(
          col("event_time"),
          col("event_type"),
          col("product_id"),
          col("category_id"),
          col("category_code"),
          col("brand"),
          col("price"),
          col("user_id"),
          col("user_session"),
          lit(reason).alias("failure_reason"),
          current_timestamp().alias("failure_timestamp"),
          lit(batchId).alias("batch_id"),
          lit(retryCount).alias("retry_count")
        )
        .withColumn("year", year(col("failure_timestamp")))
        .withColumn("month", month(col("failure_timestamp")))
        .withColumn("day", dayofmonth(col("failure_timestamp")))

      MinioUtils.writeDeltaTable(
        df = dlqRecords,
        bucketName = bucketName,
        path = path,
        saveMode = SaveMode.Append,
        partitionColumns = Some(Seq("year", "month", "day"))
      )

      val count = records.count()
      logger.warn(s"Wrote $count records to DLQ at $path (batch_id=$batchId, reason=$reason)")
    } catch {
      case e: Exception =>
        logger.error(s"CRITICAL: Failed to write to DLQ at $path", e)
    }
  }

  def readFromDLQ(
                   spark: SparkSession,
                   bucketName: String,
                   path: String
                 ): DataFrame = {
    MinioUtils.readDeltaTable(
      spark = spark,
      bucketName = bucketName,
      path = path
    )
  }

  def reprocessFromDLQ(
                        spark: SparkSession,
                        bucketName: String,
                        dlqPath: String,
                        outputHandler: DataFrame => Unit,
                        maxRetryCount: Int = 3
                      ): Unit = {
    try {
      val dlqRecords = readFromDLQ(spark, bucketName, dlqPath)
        .filter(col("retry_count") < maxRetryCount)

      if (dlqRecords.isEmpty) {
        logger.info("No records to reprocess from DLQ")
        return
      }

      // Extract only the original event columns (excluding DLQ metadata)
      val recordsToRetry = dlqRecords.select(
        col("event_time"),
        col("event_type"),
        col("product_id"),
        col("category_id"),
        col("category_code"),
        col("brand"),
        col("price"),
        col("user_id"),
        col("user_session")
      )

      val recordCount = recordsToRetry.count()
      logger.info(s"Attempting to reprocess $recordCount records from DLQ")
      outputHandler(recordsToRetry)

      // If successful, archive the processed records
      val archivePath = dlqPath.replace("/dlq/", "/dlq_archive/")
      MinioUtils.writeDeltaTable(
        df = dlqRecords,
        bucketName = bucketName,
        path = archivePath,
        saveMode = SaveMode.Append
      )
      logger.info(s"Archived reprocessed DLQ records to $archivePath")

    } catch {
      case e: Exception =>
        logger.error("Failed to reprocess records from DLQ", e)
        throw e
    }
  }
}
