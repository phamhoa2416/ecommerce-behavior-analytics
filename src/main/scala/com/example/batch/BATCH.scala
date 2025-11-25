package com.example.batch

import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.{MinioUtils, SparkUtils}
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.SaveMode

object BATCH {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Kafka-To-Minio-Batch")
    MinioUtils.configureSparkForMinio(spark)
    val minioConfig = MinioUtils.loadConfig()

    // Read from Kafka
    val kafkaDF = spark.read
      .format("kafka")
      .options(SparkUtils.kafkaOptions)
      .load()

    // Parse and normalize events
    val parsedDF = Parser.parseData(kafkaDF, Schema.schema)

    // Add partition columns for better data organization
    val enrichedDF = addPartitionColumns(parsedDF)

    // Write to MinIO with partitioning
    val writer = enrichedDF.write
      .format(minioConfig.fileFormat)
//      .mode(SaveMode.valueOf(minioConfig.writeMode))
      .option("path", minioConfig.basePath)

    // Apply partitioning if columns exist
//    if (minioConfig.partitionColumns.nonEmpty) {
//      val existingPartitionCols = minioConfig.partitionColumns.filter(colName =>
//        enrichedDF.columns.contains(colName)
//      )
//      if (existingPartitionCols.nonEmpty) {
//        writer.partitionBy(existingPartitionCols: _*)
//      }
//    }

    writer.save()

    spark.stop()
  }

  private def addPartitionColumns(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    // Add date partition column from event_time if it exists
    if (df.columns.contains("event_time")) {
      df.withColumn("date", date_format(col("event_time"), "yyyy-MM-dd"))
    } else {
      df
    }
  }
}

