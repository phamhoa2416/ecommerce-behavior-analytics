package com.example.streaming

import com.example.AppConfig
import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.SparkUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.slf4j.LoggerFactory

import java.util.Properties

object STREAMING {
  private val logger = LoggerFactory.getLogger(getClass)

  val kafkaBootstrapServer: String = AppConfig.KAFKA_BOOTSTRAP_SERVERS
  val kafkaTopic: String = AppConfig.KAFKA_STREAM_TOPIC
  val kafkaGroupId: String = AppConfig.KAFKA_GROUP_ID
  val kafkaStartingOffsets: String = AppConfig.KAFKA_STARTING_OFFSETS

  val clickhouseUrl: String = AppConfig.CLICKHOUSE_URL
  val clickhouseUser: String = AppConfig.CLICKHOUSE_USER
  val clickhousePassword: String = AppConfig.CLICKHOUSE_PASSWORD
  val clickhouseTable: String = AppConfig.CLICKHOUSE_TABLE


  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Streaming")

    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", kafkaStartingOffsets)
      .option("failOnDataLoss", "false")
      .load()

    val parsedStream = Parser.parse(
      kafkaDf,
      Schema.schema,
      AppConfig.SPARK_TIMESTAMP_PATTERN,
      AppConfig.SPARK_TIMEZONE
    ).withWatermark("event_time", "5 minutes")
      .dropDuplicates("event_time", "user_id", "product_id", "event_type")

    val checkpointLocation = AppConfig.KAFKA_CHECKPOINT_LOCATION

    val connection = new Properties()
    connection.put("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    connection.put("user", clickhouseUser)
    connection.put("password", clickhousePassword)
    connection.put("batchsize", AppConfig.CLICKHOUSE_BATCH_SIZE.toString)

    val query: StreamingQuery = parsedStream.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        if (!batchDF.isEmpty) {
          logger.info(s"Writing batch of ${batchDF.count()} records to ClickHouse table $clickhouseTable")
          batchDF.write
            .mode("append")
            .jdbc(clickhouseUrl, clickhouseTable, connection)
        } else {
          logger.debug("Skipping empty micro-batch.")
        }
      }
      .start()

    query.awaitTermination()
  }
}

