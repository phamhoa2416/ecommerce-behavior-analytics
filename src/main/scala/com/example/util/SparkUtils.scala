package com.example.util

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def createSparkSession(appName: String = "Structured Streaming Consumer"): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.clickhouse:clickhouse-jdbc:0.6.4"
      )
      .getOrCreate()
  }

  def kafkaOptions: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "subscribe" -> sys.env.getOrElse("KAFKA_TOPIC", "ecommerce_data"),
    "startingOffsets" -> sys.env.getOrElse("KAFKA_STARTING_OFFSETS", "earliest"),
    "failOnDataLoss" -> "false"
  )

  def clickhouseOptions: Map[String, String] = Map(
    "url" -> sys.env.getOrElse("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://localhost:8123/ecommerce"),
    "dbtable" -> sys.env.getOrElse("CLICKHOUSE_TABLE", "ecommerce_events"),
    "user" -> sys.env.getOrElse("CLICKHOUSE_USER", "admin"),
    "password" -> sys.env.getOrElse("CLICKHOUSE_PASSWORD", "password"),
    "driver" -> "com.clickhouse.jdbc.ClickHouseDriver"
  )

  def configureS3Support(spark: SparkSession): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    
    // MinIO configuration based on docker-compose.yaml
    // Default: http://minio:9000 (for container) or http://localhost:9000 (for local)
    val endpoint = sys.env.getOrElse("MINIO_ENDPOINT", "http://localhost:9000")
    val accessKey = sys.env.getOrElse("MINIO_ROOT_USER", sys.env.getOrElse("MINIO_ACCESS_KEY", "admin"))
    val secretKey = sys.env.getOrElse("MINIO_ROOT_PASSWORD", sys.env.getOrElse("MINIO_SECRET_KEY", "password"))
    
    hadoopConf.set("fs.s3a.endpoint", endpoint)
    hadoopConf.set("fs.s3a.access.key", accessKey)
    hadoopConf.set("fs.s3a.secret.key", secretKey)
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.connection.maximum", "15")
    hadoopConf.set("fs.s3a.attempts.maximum", "3")
  }
}

