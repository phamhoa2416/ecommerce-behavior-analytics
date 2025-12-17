package com.example.lineage

import com.example.util.MinioUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format}
import org.slf4j.LoggerFactory

object LineageTracker {
  private val logger = LoggerFactory.getLogger(getClass)

  def log(
           spark: SparkSession,
           bucketName: String,
           lineagePath: String,
           pipeline: String,
           batchId: String,
           source: String,
           sink: String,
           metrics: Map[String, Long],
           status: String,
           mode: String,
           message: String = ""
         ): Unit = {
    try {
      val df: DataFrame = spark.createDataFrame(Seq(
          (
            batchId,
            pipeline,
            source,
            sink,
            mode,
            status,
            metrics.getOrElse("total", 0L),
            metrics.getOrElse("valid", 0L),
            metrics.getOrElse("invalid", 0L),
            message
          )
        )).toDF(
          "batch_id",
          "pipeline",
          "source",
          "sink",
          "mode",
          "status",
          "total_records",
          "valid_records",
          "invalid_records",
          "message"
        )
        .withColumn("lineage_timestamp", current_timestamp())
        .withColumn("lineage_date", date_format(col("lineage_timestamp"), "yyyy-MM-dd"))

      MinioUtils.writeDeltaTable(
        df = df,
        bucketName = bucketName,
        path = lineagePath,
        saveMode = SaveMode.Append,
        partitionColumns = Some(Seq("lineage_date")),
      )
    } catch {
      case ex: Exception =>
        logger.error(s"[lineage] Failed to write lineage record for pipeline=$pipeline batch=$batchId", ex)
    }
  }
}
