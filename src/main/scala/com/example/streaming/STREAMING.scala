package com.example.streaming

import com.example.parser.Parser
import com.example.schema.Schema
import com.example.util.SparkUtils
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.streaming.StreamingQuery

object STREAMING {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession("Structured Streaming Consumer")
    val schema = Schema.schema

    val kafkaStream = spark.readStream
      .format("kafka")
      .options(SparkUtils.kafkaOptions)
      .load()

    val parsedStream = Parser.parseData(kafkaStream, schema)

    val checkpointLocation = sys.env.getOrElse("CHECKPOINT_LOCATION", "/tmp/checkpoint_clickhouse")

    val query: StreamingQuery = parsedStream.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch { (batchDF: Dataset[Row], _: Long) =>
        batchDF.write
          .format("jdbc")
          .mode("append")
          .options(SparkUtils.clickhouseOptions)
          .save()
      }
      .start()

    query.awaitTermination()
  }
}

