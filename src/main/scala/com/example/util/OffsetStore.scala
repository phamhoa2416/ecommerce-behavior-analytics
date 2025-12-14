package com.example.util

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object OffsetStore {
  private val logger = LoggerFactory.getLogger(getClass)

  def getLastOffset(
                     spark: SparkSession,
                     bucketName: String,
                     offsetPath: String,
                     topic: String
                   ): Map[Int, String] = {

    try {
      val fullPath = s"s3a://$bucketName/$offsetPath"

      val offsetTableExists = Try {
        DeltaTable.forPath(spark, fullPath)
      }.isSuccess

      if (!offsetTableExists) return Map.empty

      val deltaTable = DeltaTable.forPath(spark, fullPath)
      val offsetDf = deltaTable.toDF
        .filter(col("topic") === topic)
        .select(
          col("partition"),
          col("offset")
        )
        .groupBy("partition")
        .agg(max("offset").alias("last_offset"))
        .collect()

      offsetDf.map { row =>
        val partition = row.getAs[Int]("partition")
        val offset = row.getAs[String]("last_offset")
        partition -> offset
      }.toMap

    } catch {
      case ex: Exception =>
        logger.warn(s"Error reading offsets for topic=$topic from $offsetPath", ex)
        Map.empty
    }
  }

  def saveOffsets(
                   spark: SparkSession,
                   bucketName: String,
                   offsetPath: String,
                   offsets: Map[(String, Int), String],
                   timestamp: Long = System.currentTimeMillis()
                 ): Try[Unit] = {

    try {
      if (offsets.isEmpty) {
        logger.warn("No offsets to save")
        return Success(())
      }

      val fullPath = s"s3a://$bucketName/$offsetPath"

      import spark.implicits._
      val offsetRecords = offsets.map { case ((topic, partition), offset) =>
        (topic, partition, offset, timestamp)
      }.toSeq.toDF("topic", "partition", "offset", "processed_timestamp")

      val tableExists = Try {
        DeltaTable.forPath(spark, fullPath)
      }.isSuccess

      if (!tableExists) {
        logger.info(s"Creating offset table at $fullPath")
        MinioUtils.writeDeltaTable(
          df = offsetRecords,
          bucketName = bucketName,
          path = offsetPath,
          saveMode = org.apache.spark.sql.SaveMode.Overwrite
        )

      } else {
        val deltaTable = DeltaTable.forPath(spark, fullPath)

        deltaTable
          .as("target")
          .merge(
            offsetRecords.as("source"),
            "target.topic = source.topic AND target.partition = source.partition"
          )
          .whenMatched()
          .updateExpr(
            Map(
              "offset"               -> "source.offset",
              "processed_timestamp"  -> "source.processed_timestamp"
            )
          )
          .whenNotMatched()
          .insertAll()
          .execute()
      }

      logger.info(s"Saved ${offsets.size} offsets")
      Success(())

    } catch {
      case ex: Exception =>
        logger.error(s"Failed to save offsets", ex)
        Failure(ex)
    }
  }

  def saveOffsetsFromKafka(
                            spark: SparkSession,
                            bucketName: String,
                            offsetPath: String,
                            kafkaDf: DataFrame,
                            topic: String
                          ): Try[Unit] = {

    try {

      val offsetsDF = kafkaDf
        .select(
          col("partition").cast("int").alias("partition"),
          col("offset").cast("string").alias("offset")
        )
        .groupBy("partition")
        .agg(max(col("offset")).alias("max_offset"))
        .select(
          lit(topic).alias("topic"),
          col("partition"),
          col("max_offset").alias("offset"),
          lit(System.currentTimeMillis()).alias("processed_timestamp")
        )

      if (offsetsDF.isEmpty) {
        logger.warn("No offsets to save from Kafka DataFrame")
        return Success(())
      }

      val offsets = offsetsDF.collect().map { row =>
        val partition = row.getAs[Int]("partition")
        val offset = row.getAs[String]("offset")
        (topic, partition) -> offset
      }.toMap

      saveOffsets(spark, bucketName, offsetPath, offsets)

    } catch {
      case ex: Exception =>
        logger.error(s"Failed to extract and save offsets from Kafka DataFrame", ex)
        Failure(ex)
    }
  }
}