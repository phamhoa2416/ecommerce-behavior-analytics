package com.example.streaming

import com.example.AppConfig
import com.example.parser.Parser
import com.example.schema.Schema
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

import java.util.Properties

object STREAMING {
  val kafkaBootstrapServer: String = AppConfig.KAFKA_BOOTSTRAP_SERVERS
  val kafkaTopic: String = AppConfig.KAFKA_BATCH_TOPIC
  val kafkaGroupId: String = AppConfig.KAFKA_GROUP_ID
  val kafkaStartingOffsets: String = AppConfig.KAFKA_STARTING_OFFSETS

  val clickhouseUrl: String = AppConfig.CLICKHOUSE_URL
  val clickhouseUser: String = AppConfig.CLICKHOUSE_USER
  val clickhousePassword: String = AppConfig.CLICKHOUSE_PASSWORD
  val clickhouseTable: String = AppConfig.CLICKHOUSE_TABLE


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Streaming")
      .master("local[*]")
      .getOrCreate()

    val kafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", kafkaStartingOffsets)
      .option("failOnDataLoss", "false")
      .load()

    val parsedStream = Parser.parseData(kafkaDf, Schema.schema)

    val checkpointLocation = sys.env.getOrElse("CHECKPOINT_LOCATION", "/tmp/checkpoint")

    val connection = new Properties()
    connection.put("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    connection.put("user", clickhouseUser)
    connection.put("password", clickhousePassword)

    val query: StreamingQuery = parsedStream.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch { (batchDF: Dataset[Row], _: Long) =>
        if (!batchDF.isEmpty) {
          batchDF.write
            .mode("append")
            .jdbc(clickhouseUrl, clickhouseTable, connection)
        }
      }
      .start()

    query.awaitTermination()
  }
}

