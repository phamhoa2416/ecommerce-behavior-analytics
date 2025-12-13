package com.example.tool

import com.example.AppConfig
import com.example.handler.RetryHandler
import com.example.schema.Schema
import com.example.util.SparkUtils
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import scala.util.Try

object CSVToKafka {
  private val logger = LoggerFactory.getLogger(getClass)

  private final case class Options(
    inputPath: String,
    topic: String,
    partitions: Option[Int],
    maxPartitionMb: Int
  )

  private def parseArgs(args: Array[String]): Options = {
    val defaultInput = Paths.get("data/2019-Oct-1.csv").toString
    val inputPath = args.headOption.getOrElse(defaultInput)
    val topic = args.lift(1).getOrElse(AppConfig.KAFKA_BATCH_TOPIC)
    val partitions = args.lift(2).flatMap(v => Try(v.toInt).toOption)
    val maxPartitionMb = args.lift(3).flatMap(v => Try(v.toInt).toOption).getOrElse(16)
    Options(inputPath, topic, partitions, maxPartitionMb)
  }

  def main(args: Array[String]): Unit = {
    val opts = parseArgs(args)

    if (!Files.exists(Paths.get(opts.inputPath))) {
      logger.error(s"Input file not found: ${opts.inputPath}")
      sys.exit(1)
    }

    val spark = SparkUtils.createSparkSession("CsvToKafkaProducer")

    // Small file splits to keep task memory low; avoid extra shuffle unless user requests partitions.
    val maxPartitionBytes = s"${opts.maxPartitionMb}m"
    spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytes)
    spark.conf.set("spark.sql.files.openCostInBytes", maxPartitionBytes)
    opts.partitions.foreach(p => spark.conf.set("spark.sql.shuffle.partitions", p))

    logger.info(s"Reading CSV from ${opts.inputPath}")
    logger.info(s"Target Kafka topic: ${opts.topic}; bootstrap: ${AppConfig.KAFKA_BOOTSTRAP_SERVERS}; partitions: ${opts.partitions.getOrElse(AppConfig.SPARK_SHUFFLE_PARTITIONS)}; maxPartitionMb: ${opts.maxPartitionMb}")

    val baseDf = spark.read
      .schema(Schema.schema)
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(opts.inputPath)

    // Use coalesce to limit tasks without full shuffle; repartition only if caller asks explicitly.
    val csvDf = opts.partitions match {
      case Some(p) if p > 0 => baseDf.coalesce(p)
      case _ => baseDf
    }

    val kafkaPayload = csvDf
      .select(to_json(struct(csvDf.columns.map(col): _*)).alias("value"))
      .selectExpr("CAST(null AS STRING) AS key", "value")

    val kafkaOptions = Map(
      "kafka.bootstrap.servers" -> AppConfig.KAFKA_BOOTSTRAP_SERVERS,
      "topic" -> opts.topic,
      // Producer tuning: allow slightly larger batches and linger for better throughput on large inputs.
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

    spark.stop()
  }
}
