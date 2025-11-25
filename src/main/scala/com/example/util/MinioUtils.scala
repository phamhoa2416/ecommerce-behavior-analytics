package com.example.util

import org.apache.spark.sql.SparkSession

case class MinioConfig(
  bucketName: String,
  basePath: String,
  fileFormat: String,
//  writeMode: String,
//  partitionColumns: Seq[String]
)

object MinioUtils {
  def loadConfig(): MinioConfig = {
    val bucketName = sys.env.getOrElse("MINIO_BUCKET_NAME", "ecommerce-bucket")
    val basePath = sys.env.getOrElse("MINIO_BASE_PATH", "ecommerce/events")
    val fullPath = s"s3a://$bucketName/$basePath"
    
    MinioConfig(
      bucketName = bucketName,
      basePath = fullPath,
      fileFormat = sys.env.getOrElse("MINIO_FILE_FORMAT", "parquet")
//      writeMode = sys.env.getOrElse("MINIO_WRITE_MODE", "Append"),
//      partitionColumns = sys.env.get("MINIO_PARTITION_COLUMNS")
//        .map(_.split(",").map(_.trim).toSeq)
//        .getOrElse(Seq("event_type", "date"))
    )
  }

  def configureSparkForMinio(spark: SparkSession): Unit = {
    SparkUtils.configureS3Support(spark)
  }
}

