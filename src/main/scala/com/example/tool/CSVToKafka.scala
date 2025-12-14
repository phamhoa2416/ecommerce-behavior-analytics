package com.example.tool

import com.example.AppConfig
import com.example.handler.RetryHandler
import com.example.schema.Schema
import com.example.util.SparkUtils
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}

object CSVToKafka {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(
    inputPath: String,
    topic: String = AppConfig.KAFKA_BATCH_TOPIC,
    partitions: Option[Int] = None,
    maxPartitionMb: Int = 16
  ): Unit = {
    if (!Files.exists(Paths.get(inputPath))) {
      throw new IllegalArgumentException(s"Input file not found: $inputPath")
    }

    val spark = SparkUtils.createSparkSession("CsvToKafkaProducer")

    try {
      val maxPartitionBytes = s"${maxPartitionMb}m"
      spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytes)
      spark.conf.set("spark.sql.files.openCostInBytes", maxPartitionBytes)
      partitions.foreach(p => spark.conf.set("spark.sql.shuffle.partitions", p))

      logger.info(s"Reading CSV from $inputPath")
      logger.info(s"Target Kafka topic: $topic; bootstrap: ${AppConfig.KAFKA_BOOTSTRAP_SERVERS}; partitions: ${partitions.getOrElse(AppConfig.SPARK_SHUFFLE_PARTITIONS)}; maxPartitionMb: $maxPartitionMb")

      val baseDf = spark.read
        .schema(Schema.schema)
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .csv(inputPath)

      val csvDf = partitions match {
        case Some(p) if p > 0 => baseDf.coalesce(p)
        case _ => baseDf
      }

      val kafkaPayload = csvDf
        .select(to_json(struct(csvDf.columns.map(col): _*)).alias("value"))
        .selectExpr("CAST(null AS STRING) AS key", "value")

      val kafkaOptions = Map(
        "kafka.bootstrap.servers" -> AppConfig.KAFKA_BOOTSTRAP_SERVERS,
        "topic" -> topic,
        "kafka.linger.ms" -> "500",
        "kafka.batch.size" -> "131072",
        "kafka.max.request.size" -> "1048576"
      )

      RetryHandler.withRetry(
        kafkaPayload.write
          .format("kafka")
          .options(kafkaOptions)
          .save(),
        name = "Kafka bulk CSV publish"
      ) match {
        case scala.util.Success(_) =>
          logger.info("CSV pushed to Kafka successfully")
        case scala.util.Failure(ex) =>
          logger.error("Failed to push CSV to Kafka", ex)
          throw ex
      }
    } finally {
      logger.info("Stopping Spark session. Application complete")
      spark.stop()
    }
  }
}
