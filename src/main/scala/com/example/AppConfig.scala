package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.util.Try

//noinspection ScalaUnusedSymbol
object AppConfig {
  private val logger = LoggerFactory.getLogger(getClass)

  final case class KafkaSettings(
    bootstrapServers: String,
    batchTopic: String,
    streamTopic: String,
    groupId: String,
    checkpointLocation: String,
    startingOffsets: String,
  )

  final case class MinioSettings(
    endpoint: String,
    accessKey: String,
    secretKey: String,
    bucketName: String,
    basePath: String,
    fileFormat: String,
    pathStyleAccess: String
  )

  final case class ClickhouseSettings(
    url: String,
    user: String,
    password: String,
    database: String,
    table: String,
    batchSize: Int
  )

  final case class SparkSettings(
    master: String,
    shufflePartitions: Int,
    timestampPattern: String,
    timezone: String
  )

  final case class ALSEventWeight(
    view: Double,
    cart: Double,
    removeFromCart: Double,
    purchase: Double,
    viewCap: Double,
  )

  final case class ALSConfig(
    base_path: String,
    rank: Int,
    iter: Int,
    regParam: Double,
    alpha: Double,
    K: Int,
    eventWeight: ALSEventWeight
  )

  final case class SparkMLConfig(
    timeDecayFactor: Double,
    als: ALSConfig
  )

  final case class ApplicationConfig(
    kafka: KafkaSettings,
    minio: MinioSettings,
    clickhouse: ClickhouseSettings,
    spark: SparkSettings,
    ml: SparkMLConfig
  )

  private val envStats: String = sys.env.getOrElse("ENV_JOB_RUN", "env")
  logger.info(s"Loading configuration for environment: $envStats")

  val config: Config = {
    val fileName = if (envStats == "prod") "application.prod.conf" else "application.dev.conf"
    Try(ConfigFactory.load(fileName)).recover {
      case ex: Exception =>
        logger.error(s"Failed to load $fileName", ex)
        throw ex
    }.get
  }

  private val kafkaConfig = config.getConfig("kafka")
  private val minioConfig = config.getConfig("minio")
  private val clickhouseConfig = config.getConfig("clickhouse")
  private val sparkConfig = config.getConfig("spark")
  private val sparkMlConfig = config.getConfig("ml")
  private val alsConfig = sparkMlConfig.getConfig("als")
  private val alsEventWeightConfig = alsConfig.getConfig("event_weights")

  private def requireNonEmpty(value: String, key: String): String = {
    if (value == null || value.trim.isEmpty) {
      throw new IllegalArgumentException(s"Configuration value for '$key' must not be empty")
    }
    value.trim
  }

  private def envOrConfig(envKey: String, defaultValue: => String): String = {
    val resolved = sys.env.get(envKey).filter(_.nonEmpty).getOrElse(defaultValue)
    requireNonEmpty(resolved, envKey)
  }

  private def envOrConfigInt(envKey: String, defaultValue: => Int): Int = {
    sys.env.get(envKey).flatMap(v => Try(v.toInt).toOption)
      .getOrElse(defaultValue)
  }

  private def envOrConfigDouble(envKey: String, defaultValue: => Double): Double = {
    sys.env.get(envKey).flatMap(v => Try(v.toDouble).toOption)
      .getOrElse(defaultValue)
  }

  private val kafkaSettings: KafkaSettings = KafkaSettings(
    bootstrapServers = envOrConfig("KAFKA_BOOTSTRAP_SERVERS", kafkaConfig.getString("bootstrap_servers")),
    batchTopic = envOrConfig("KAFKA_TOPIC", kafkaConfig.getString("batch_topic")),
    streamTopic = envOrConfig("KAFKA_STREAM_TOPIC", kafkaConfig.getString("stream_topic")),
    groupId = envOrConfig("KAFKA_GROUP_ID", kafkaConfig.getString("group_id")),
    checkpointLocation = envOrConfig("KAFKA_CHECKPOINT_LOCATION", kafkaConfig.getString("checkpoint_location")),
    startingOffsets = envOrConfig("KAFKA_STARTING_OFFSETS", kafkaConfig.getString("starting_offsets"))
  )

  private val minioSettings: MinioSettings = MinioSettings(
    endpoint = envOrConfig("MINIO_ENDPOINT", minioConfig.getString("endpoint")),
    accessKey = envOrConfig("MINIO_ROOT_USER",
      sys.env.getOrElse("MINIO_ACCESS_KEY", minioConfig.getString("access_key"))
    ),
    secretKey = envOrConfig("MINIO_ROOT_PASSWORD",
      sys.env.getOrElse("MINIO_SECRET_KEY", minioConfig.getString("secret_key"))
    ),
    bucketName = envOrConfig("MINIO_BUCKET_NAME", minioConfig.getString("bucket_name")),
    basePath = envOrConfig("MINIO_BASE_PATH", minioConfig.getString("base_path")),
    fileFormat = envOrConfig("MINIO_FILE_FORMAT", minioConfig.getString("file_format")),
    pathStyleAccess = envOrConfig("MINIO_PATH_STYLE_ACCESS", minioConfig.getString("path_style_access"))
  )

  private val clickhouseSettings: ClickhouseSettings = ClickhouseSettings(
    url = envOrConfig("CLICKHOUSE_JDBC_URL", clickhouseConfig.getString("jdbc_url")),
    user = envOrConfig("CLICKHOUSE_USER", clickhouseConfig.getString("user")),
    password = envOrConfig("CLICKHOUSE_PASSWORD", clickhouseConfig.getString("password")),
    database = envOrConfig("CLICKHOUSE_DATABASE", clickhouseConfig.getString("database")),
    table = envOrConfig("CLICKHOUSE_TABLE", clickhouseConfig.getString("table")),
    batchSize = envOrConfigInt("CLICKHOUSE_BATCH_SIZE", clickhouseConfig.getInt("batch_size"))
  )

  private val sparkSettings: SparkSettings = SparkSettings(
    master = envOrConfig("SPARK_MASTER", sparkConfig.getString("master")),
    shufflePartitions = envOrConfigInt("SPARK_SHUFFLE_PARTITIONS", sparkConfig.getInt("shuffle_partitions")),
    timestampPattern = envOrConfig("SPARK_TIMESTAMP_PATTERN", sparkConfig.getString("timestamp_pattern")),
    timezone = envOrConfig("SPARK_TIMEZONE", sparkConfig.getString("timezone"))
  )

  private val sparkMlSettings: SparkMLConfig = SparkMLConfig(
    timeDecayFactor = envOrConfigDouble("SPARK_ML_TIME_DECAY_FACTOR", sparkMlConfig.getDouble("time_decay_factor")),
    als = ALSConfig(
      base_path = envOrConfig("SPARK_ML_ALS_BASE_PATH", alsConfig.getString("base_path")),
      rank = envOrConfigInt("SPARK_ML_ALS_RANK", alsConfig.getInt("rank")),
      iter = envOrConfigInt("SPARK_ML_ALS_MAX_ITERATION", alsConfig.getInt("iter")),
      regParam = envOrConfigDouble("SPARK_ML_ALS_REGULARIZATION", alsConfig.getDouble("reg_param")),
      alpha = envOrConfigDouble("SPARK_ML_ALS_IMPLICIT_ALPHA", alsConfig.getDouble("alpha")),
      K = envOrConfigInt("SPARK_ML_ALS_EVALUATION_TOP_K", alsConfig.getInt("evaluation_top_K")),
      eventWeight = ALSEventWeight(
        view = envOrConfigDouble("SPARK_ML_ALS_EVENT_WEIGHT_VIEW", alsEventWeightConfig.getDouble("view")),
        cart = envOrConfigDouble("SPARK_ML_ALS_EVENT_WEIGHT_CART", alsEventWeightConfig.getDouble("cart")),
        removeFromCart = envOrConfigDouble("SPARK_ML_ALS_EVENT_WEIGHT_REMOVE_FROM_CART", alsEventWeightConfig.getDouble("remove_from_cart")),
        purchase = envOrConfigDouble("SPARK_ML_ALS_EVENT_WEIGHT_PURCHASE", alsEventWeightConfig.getDouble("purchase")),
        viewCap = envOrConfigDouble("SPARK_ML_ALS_EVENT_WEIGHT_VIEW_CAP", alsEventWeightConfig.getDouble("view_cap"))
      )
    )
  )

  // Backwards compatible fields
  val KAFKA_BOOTSTRAP_SERVERS: String = kafkaSettings.bootstrapServers
  val KAFKA_BATCH_TOPIC: String = kafkaSettings.batchTopic
  val KAFKA_STREAM_TOPIC: String = kafkaSettings.streamTopic
  val KAFKA_GROUP_ID: String = kafkaSettings.groupId
  val KAFKA_CHECKPOINT_LOCATION: String = kafkaSettings.checkpointLocation
  val KAFKA_STARTING_OFFSETS: String = kafkaSettings.startingOffsets

  val MINIO_ENDPOINT: String = minioSettings.endpoint
  val MINIO_ACCESS_KEY: String = minioSettings.accessKey
  val MINIO_SECRET_KEY: String = minioSettings.secretKey
  val MINIO_BUCKET_NAME: String = minioSettings.bucketName
  val MINIO_BASE_PATH: String = minioSettings.basePath
  val MINIO_FILE_FORMAT: String = minioSettings.fileFormat
  val MINIO_PATH_STYLE_ACCESS: String = minioSettings.pathStyleAccess

  val CLICKHOUSE_URL: String = clickhouseSettings.url
  val CLICKHOUSE_USER: String = clickhouseSettings.user
  val CLICKHOUSE_PASSWORD: String = clickhouseSettings.password
  val CLICKHOUSE_DATABASE: String = clickhouseSettings.database
  val CLICKHOUSE_TABLE: String = clickhouseSettings.table
  val CLICKHOUSE_BATCH_SIZE: Int = clickhouseSettings.batchSize

  val SPARK_MASTER: String = sparkSettings.master
  val SPARK_SHUFFLE_PARTITIONS: Int = sparkSettings.shufflePartitions
  val SPARK_TIMESTAMP_PATTERN: String = sparkSettings.timestampPattern
  val SPARK_TIMEZONE: String = sparkSettings.timezone

  val SPARK_ML_TIME_DECAY_FACTOR: Double = sparkMlSettings.timeDecayFactor
  val SPARK_ML_ALS_RANK: Int = sparkMlSettings.als.rank
  val SPARK_ML_ALS_MAX_ITERATION: Int = sparkMlSettings.als.iter
  val SPARK_ML_ALS_REGULARIZATION: Double = sparkMlSettings.als.regParam
  val SPARK_ML_ALS_IMPLICIT_ALPHA: Double = sparkMlSettings.als.alpha
  val SPARK_ML_ALS_EVALUATION_TOP_K: Int = sparkMlSettings.als.K
  val SPARK_ML_ALS_BASE_PATH: String = sparkMlSettings.als.base_path
  val SPARK_ML_ALS_EVENT_WEIGHT_VIEW: Double = sparkMlSettings.als.eventWeight.view
  val SPARK_ML_ALS_EVENT_WEIGHT_CART: Double = sparkMlSettings.als.eventWeight.cart
  val SPARK_ML_ALS_EVENT_WEIGHT_REMOVE_FROM_CART: Double = sparkMlSettings.als.eventWeight.removeFromCart
  val SPARK_ML_ALS_EVENT_WEIGHT_PURCHASE: Double = sparkMlSettings.als.eventWeight.purchase
  val SPARK_ML_ALS_EVENT_WEIGHT_VIEW_CAP: Double = sparkMlSettings.als.eventWeight.viewCap

  /** Convenience accessor that returns the full configuration as a single value. */
  val applicationConfig: ApplicationConfig =
    ApplicationConfig(kafkaSettings, minioSettings, clickhouseSettings, sparkSettings, sparkMlSettings)
}
