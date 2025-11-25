package com.example.batch

import com.example.AppConfig
import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.Utils
import org.apache.spark.sql.functions.{col, dayofmonth, month, year}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.{Failure, Success}

object BATCH {

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServer = AppConfig.KAFKA_BOOTSTRAP_SERVERS
    val kafkaTopic = AppConfig.KAFKA_BATCH_TOPIC
    val kafkaGroupId = AppConfig.KAFKA_GROUP_ID
    val kafkaStartingOffsets = AppConfig.KAFKA_STARTING_OFFSETS

    val minioEndpoint = AppConfig.MINIO_ENDPOINT
    val minioAccessKey = AppConfig.MINIO_ACCESS_KEY
    val minioSecretKey = AppConfig.MINIO_SECRET_KEY
    val minioBucketName = AppConfig.MINIO_BUCKET_NAME
    val minioPathStyleAccess = AppConfig.MINIO_PATH_STYLE_ACCESS

    val spark = SparkSession.builder()
      .appName("Batch")
      .master("local[*]")
      .getOrCreate()

    try {
      Utils.configureMinIO(spark, minioEndpoint, minioAccessKey, minioSecretKey, minioPathStyleAccess)
      Utils.checkBucketExists(minioEndpoint, minioAccessKey, minioSecretKey, minioBucketName) match {
        case Success(_) => println(s"MinIO bucket '$minioBucketName' is ready.")
        case Failure(exception) =>
          println(s"Error while checking/creating MinIO bucket: ${exception.getMessage}")
          sys.exit(1)
      }

      val kafkaDf = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrapServer)
        .option("subscribe", kafkaTopic)
        .option("startingOffsets", kafkaStartingOffsets)
        .option("kafka.group.id", kafkaGroupId)
        .load()

      println(s"Reading from Kafka topic: $kafkaTopic")

      val parsedDF = Parser.parseData(kafkaDf, Schema.schema)

      val dataWithPartition = parsedDF
        .withColumn("year", year(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))

      println(s"Parsed records: ${dataWithPartition.count()}")

      val minioPath = s"s3a://$minioBucketName/ecommerce-batch-data"

      println(s"Writing data to MinIO: $minioPath")
      dataWithPartition
        .repartition(col("year"), col("month"), col("day"))
        .write
        .mode(SaveMode.Append)
        .partitionBy("year", "month", "day")
        .parquet(minioPath)

      println("Batch job completed successfully!")
    }
  }
}

