package com.example

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  val envStats: String = sys.env.getOrElse("ENV_JOB_RUN", "env")
  val config: Config = if (envStats == "prod") { ConfigFactory.load("application.prod.conf") }
  else { ConfigFactory.load("application.dev.conf") }

  // Kafka Configuration
  val kafka: Config = config.getConfig("kafka")
  val KAFKA_BOOTSTRAP_SERVERS: String = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", kafka.getString("bootstrap_servers"))
  val KAFKA_BATCH_TOPIC: String = sys.env.getOrElse("KAFKA_TOPIC", kafka.getString("batch_topic"))
  val KAFKA_STREAM_TOPIC: String = sys.env.getOrElse("KAFKA_STREAM_TOPIC", kafka.getString("stream_topic"))
  val KAFKA_GROUP_ID: String = sys.env.getOrElse("KAFKA_GROUP_ID", kafka.getString("group_id"))
  val KAFKA_CHECKPOINT_LOCATION: String = sys.env.getOrElse("KAFKA_CHECKPOINT_LOCATION", kafka.getString("checkpoint_location"))
  val KAFKA_STARTING_OFFSETS: String = sys.env.getOrElse("KAFKA_STARTING_OFFSETS", kafka.getString("starting_offsets"))

  // MinIO Configuration
  val minio: Config = config.getConfig("minio")
  val MINIO_ENDPOINT: String = sys.env.getOrElse("MINIO_ENDPOINT", minio.getString("endpoint"))
  val MINIO_ACCESS_KEY: String = sys.env.getOrElse("MINIO_ROOT_USER", sys.env.getOrElse("MINIO_ACCESS_KEY", minio.getString("access_key")))
  val MINIO_SECRET_KEY: String = sys.env.getOrElse("MINIO_ROOT_PASSWORD", sys.env.getOrElse("MINIO_SECRET_KEY", minio.getString("secret_key")))
  val MINIO_BUCKET_NAME: String = sys.env.getOrElse("MINIO_BUCKET_NAME", minio.getString("bucket_name"))
  val MINIO_BASE_PATH: String = sys.env.getOrElse("MINIO_BASE_PATH", minio.getString("base_path"))
  val MINIO_FILE_FORMAT: String = sys.env.getOrElse("MINIO_FILE_FORMAT", minio.getString("file_format"))
  val MINIO_PATH_STYLE_ACCESS: String = sys.env.getOrElse("MINIO_PATH_STYLE_ACCESS", minio.getString("path_style_access"))

  // ClickHouse Configuration
  val clickhouse: Config = config.getConfig("clickhouse")
  val CLICKHOUSE_URL: String = sys.env.getOrElse("CLICKHOUSE_JDBC_URL", clickhouse.getString("jdbc_url"))
  val CLICKHOUSE_USER: String = sys.env.getOrElse("CLICKHOUSE_USER", clickhouse.getString("user"))
  val CLICKHOUSE_PASSWORD: String = sys.env.getOrElse("CLICKHOUSE_PASSWORD", clickhouse.getString("password"))
  val CLICKHOUSE_DATABASE: String = sys.env.getOrElse("CLICKHOUSE_DATABASE", clickhouse.getString("database"))
  val CLICKHOUSE_TABLE: String = sys.env.getOrElse("CLICKHOUSE_TABLE", clickhouse.getString("table"))
}
