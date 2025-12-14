package com.example.config

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

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
    pathStyleAccess: String,
    connectionMaximum: Int = 15,
    attemptsMaximum: Int = 3,
    awsRegion: String = "us-east-1",
    vacuumRetentionHours: Int = 168
  )

  final case class ClickhouseSettings(
    url: String,
    user: String,
    password: String,
    database: String,
    table: String,
    batchSize: Int,
    maxConnections: Int = 10,
    connectionTimeoutMs: Int = 30000,
    socketTimeoutMs: Int = 30000
  )

  final case class SparkSettings(
    master: String,
    shufflePartitions: Int,
    timestampPattern: String,
    timezone: String,
    watermarkDuration: String
  )

  final case class PipelineSettings(
    batchInvalidPath: String = "batch/invalid",
    batchDlqPath: String = "batch/dlq",
    batchLineagePath: String = "lineage",
    batchOffsetPath: String = "batch/offsets",
    streamingInvalidPath: String = "streaming/invalid",
    streamingDlqPath: String = "streaming/dlq",
    streamingLineagePath: String = "streaming/lineage",
    streamingDedupPath: String = "streaming/deduplication",
    dlqShutdownWaitSeconds: Int = 5,
    dedupPersistenceInterval: Int = 10,
    dedupKeyColumns: Seq[String] = Seq("event_time", "user_id", "product_id", "event_type")
  )

  final case class ValidationSettings(
    minPrice: Double = 0.01,
    maxPrice: Double = 1000000.0,
    validEventTypes: Seq[String] = Seq("view", "cart", "purchase", "remove_from_cart")
  )

  final case class DeduplicationSettings(
    maxCacheSize: Int = 1000000,
    loadHistoryHours: Int = 24,
    evictionBuffer: Int = 1000
  )

  final case class DLQSettings(
    queueSize: Int = 10000,
    offerTimeoutSeconds: Int = 5,
    pollTimeoutSeconds: Int = 1
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
    blockFactor: Int,
    eventWeight: ALSEventWeight
  )

  final case class ClsEventWeight(
    view: Double,
    cart: Double,
    removeFromCart: Double,
    purchase: Double
  )

  final case class ClsConfig(
    base_path: String,
    eventWeight: ClsEventWeight,
    trees: Int,
    maxDepth: Int,
    minInstancesPerNode: Int,
    maxBins: Int,
    subsamplingRate: Double,
    featureSubsetStrategy: String,
    useTopNBucketing: Boolean,
    topNCategories: Int,
  )

  final case class SparkMLConfig(
    timeDecayFactor: Double,
    als: ALSConfig,
    cls: ClsConfig
  )

  final case class ApplicationConfig(
    kafka: KafkaSettings,
    minio: MinioSettings,
    clickhouse: ClickhouseSettings,
    spark: SparkSettings,
    ml: SparkMLConfig,
    pipeline: PipelineSettings = PipelineSettings(),
    validation: ValidationSettings = ValidationSettings(),
    deduplication: DeduplicationSettings = DeduplicationSettings(),
    dlq: DLQSettings = DLQSettings()
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
  private val clsConfig = sparkMlConfig.getConfig("classification")
  private val clsEventWeightConfig = clsConfig.getConfig("weights")

  private val pipelineConfig = Try(config.getConfig("pipeline")).getOrElse(ConfigFactory.empty())
  private val validationConfig = Try(config.getConfig("validation")).getOrElse(ConfigFactory.empty())
  private val deduplicationConfig = Try(config.getConfig("deduplication")).getOrElse(ConfigFactory.empty())
  private val dlqConfig = Try(config.getConfig("dlq")).getOrElse(ConfigFactory.empty())

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

  private def envOrConfigSeq(envKey: String, defaultValue: => Seq[String]): Seq[String] = {
    sys.env.get(envKey).map(_.split(",").map(_.trim).toSeq)
      .getOrElse(defaultValue)
  }

  private def envOrConfigBoolean(envKey: String, defaultValue: => Boolean): Boolean = {
    sys.env.get(envKey).flatMap(v => Try(v.toBoolean).toOption)
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

  val minioSettings: MinioSettings = MinioSettings(
    endpoint = envOrConfig("MINIO_ENDPOINT", minioConfig.getString("endpoint")),
    accessKey = envOrConfig("MINIO_ACCESS_KEY",
      sys.env.getOrElse("MINIO_ROOT_USER", minioConfig.getString("access_key"))
    ),
    secretKey = envOrConfig("MINIO_SECRET_KEY",
      sys.env.getOrElse("MINIO_ROOT_PASSWORD", minioConfig.getString("secret_key"))
    ),
    bucketName = envOrConfig("MINIO_BUCKET_NAME", minioConfig.getString("bucket_name")),
    basePath = envOrConfig("MINIO_BASE_PATH", minioConfig.getString("base_path")),
    fileFormat = envOrConfig("MINIO_FILE_FORMAT", minioConfig.getString("file_format")),
    pathStyleAccess = envOrConfig("MINIO_PATH_STYLE_ACCESS", minioConfig.getString("path_style_access")),
    connectionMaximum = envOrConfigInt("MINIO_CONNECTION_MAXIMUM",
      Try(minioConfig.getInt("connection_maximum")).getOrElse(15)),
    attemptsMaximum = envOrConfigInt("MINIO_ATTEMPTS_MAXIMUM",
      Try(minioConfig.getInt("attempts_maximum")).getOrElse(3)),
    awsRegion = envOrConfig("MINIO_AWS_REGION",
      Try(minioConfig.getString("aws_region")).getOrElse("us-east-1")),
    vacuumRetentionHours = envOrConfigInt("MINIO_VACUUM_RETENTION_HOURS",
      Try(minioConfig.getInt("vacuum_retention_hours")).getOrElse(168))
  )

  val clickhouseSettings: ClickhouseSettings = ClickhouseSettings(
    url = envOrConfig("CLICKHOUSE_JDBC_URL", clickhouseConfig.getString("jdbc_url")),
    user = envOrConfig("CLICKHOUSE_USER", clickhouseConfig.getString("user")),
    password = envOrConfig("CLICKHOUSE_PASSWORD", clickhouseConfig.getString("password")),
    database = envOrConfig("CLICKHOUSE_DATABASE", clickhouseConfig.getString("database")),
    table = envOrConfig("CLICKHOUSE_TABLE", clickhouseConfig.getString("table")),
    batchSize = envOrConfigInt("CLICKHOUSE_BATCH_SIZE", clickhouseConfig.getInt("batch_size")),
    maxConnections = envOrConfigInt("CLICKHOUSE_MAX_CONNECTIONS",
      Try(clickhouseConfig.getInt("max_connections")).getOrElse(10)),
    connectionTimeoutMs = envOrConfigInt("CLICKHOUSE_CONNECTION_TIMEOUT_MS",
      Try(clickhouseConfig.getInt("connection_timeout_ms")).getOrElse(30000)),
    socketTimeoutMs = envOrConfigInt("CLICKHOUSE_SOCKET_TIMEOUT_MS",
      Try(clickhouseConfig.getInt("socket_timeout_ms")).getOrElse(30000))
  )

  private val sparkSettings: SparkSettings = SparkSettings(
    master = envOrConfig("SPARK_MASTER", sparkConfig.getString("master")),
    shufflePartitions = envOrConfigInt("SPARK_SHUFFLE_PARTITIONS", sparkConfig.getInt("shuffle_partitions")),
    timestampPattern = envOrConfig("SPARK_TIMESTAMP_PATTERN", sparkConfig.getString("timestamp_pattern")),
    timezone = envOrConfig("SPARK_TIMEZONE", sparkConfig.getString("timezone")),
    watermarkDuration = envOrConfig("SPARK_WATERMARK_DURATION",
      Try(sparkConfig.getString("watermark_duration")).getOrElse("5 minutes"))
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
      blockFactor = envOrConfigInt("SPARK_ML_ALS_BLOCK_FACTOR", alsConfig.getInt("block_factor")),
      eventWeight = ALSEventWeight(
        view = envOrConfigDouble("SPARK_ML_ALS_EVENT_WEIGHT_VIEW", alsEventWeightConfig.getDouble("view")),
        cart = envOrConfigDouble("SPARK_ML_ALS_EVENT_WEIGHT_CART", alsEventWeightConfig.getDouble("cart")),
        removeFromCart = envOrConfigDouble("SPARK_ML_ALS_EVENT_WEIGHT_REMOVE_FROM_CART", alsEventWeightConfig.getDouble("remove_from_cart")),
        purchase = envOrConfigDouble("SPARK_ML_ALS_EVENT_WEIGHT_PURCHASE", alsEventWeightConfig.getDouble("purchase")),
        viewCap = envOrConfigDouble("SPARK_ML_ALS_EVENT_WEIGHT_VIEW_CAP", alsEventWeightConfig.getDouble("view_cap"))
      ),
    ),
    cls = ClsConfig(
      base_path = envOrConfig("SPARK_ML_CLS_BASE_PATH", clsConfig.getString("base_path")),
      eventWeight = ClsEventWeight(
        view = envOrConfigDouble("SPARK_ML_CLS_EVENT_WEIGHT_VIEW", clsEventWeightConfig.getDouble("view")),
        cart = envOrConfigDouble("SPARK_ML_CLS_EVENT_WEIGHT_CART", clsEventWeightConfig.getDouble("cart")),
        removeFromCart = envOrConfigDouble("SPARK_ML_CLS_EVENT_WEIGHT_REMOVE_FROM_CART", clsEventWeightConfig.getDouble("remove_from_cart")),
        purchase = envOrConfigDouble("SPARK_ML_CLS_EVENT_WEIGHT_PURCHASE", clsEventWeightConfig.getDouble("purchase"))
      ),
      trees = envOrConfigInt("SPARK_ML_CLS_TREES", clsConfig.getInt("trees")),
      maxDepth = envOrConfigInt("SPARK_ML_CLS_MAX_DEPTH", clsConfig.getInt("max_depth")),
      minInstancesPerNode = envOrConfigInt("SPARK_ML_CLS_MIN_INSTANCES_PER_NODE", clsConfig.getInt("min_instances_per_node")),
      maxBins = envOrConfigInt("SPARK_ML_CLS_MAX_BINS", clsConfig.getInt("max_bins")),
      subsamplingRate = envOrConfigDouble("SPARK_ML_CLS_SUBSAMPLING_RATE", clsConfig.getDouble("subsampling_rate")),
      featureSubsetStrategy = envOrConfig("SPARK_ML_CLS_FEATURE_SUBSET_STRATEGY", clsConfig.getString("feature_subset_strategy")),
      useTopNBucketing = envOrConfigBoolean("SPARK_ML_CLS_USE_TOP_N_BUCKETING", clsConfig.getBoolean("use_top_n_bucketing")),
      topNCategories = envOrConfigInt("SPARK_ML_CLS_TOP_N_CATEGORIES", clsConfig.getInt("top_n_categories")),
    )
  )

  // Validate topNCategories + 1 <= maxBins only when bucketing is enabled
  if (sparkMlSettings.cls.useTopNBucketing) {
    require(sparkMlSettings.cls.topNCategories + 1 <= sparkMlSettings.cls.maxBins,
      s"topNCategories (${sparkMlSettings.cls.topNCategories}) + 1 must be <= maxBins (${sparkMlSettings.cls.maxBins}) to accommodate 'other' bucket")
  }

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
  val SPARK_WATERMARK_DURATION: String = sparkSettings.watermarkDuration

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
  val SPARK_ML_ALS_BLOCK_FACTOR: Int = sparkMlSettings.als.blockFactor

  val SPARK_ML_CLS_BASE_PATH: String = sparkMlSettings.cls.base_path
  val SPARK_ML_CLS_EVENT_WEIGHT_VIEW: Double = sparkMlSettings.cls.eventWeight.view
  val SPARK_ML_CLS_EVENT_WEIGHT_CART: Double = sparkMlSettings.cls.eventWeight.cart
  val SPARK_ML_CLS_EVENT_WEIGHT_REMOVE_FROM_CART: Double = sparkMlSettings.cls.eventWeight.removeFromCart
  val SPARK_ML_CLS_EVENT_WEIGHT_PURCHASE: Double = sparkMlSettings.cls.eventWeight.purchase
  val SPARK_ML_CLS_TREES: Int = sparkMlSettings.cls.trees
  val SPARK_ML_CLS_MAX_DEPTH: Int = sparkMlSettings.cls.maxDepth
  val SPARK_ML_CLS_MIN_INSTANCES_PER_NODE: Int = sparkMlSettings.cls.minInstancesPerNode
  val SPARK_ML_CLS_MAX_BINS: Int = sparkMlSettings.cls.maxBins
  val SPARK_ML_CLS_SUBSAMPLING_RATE: Double = sparkMlSettings.cls.subsamplingRate
  val SPARK_ML_CLS_FEATURE_SUBSET_STRATEGY: String = sparkMlSettings.cls.featureSubsetStrategy
  val SPARK_ML_CLS_USE_TOP_N_BUCKETING: Boolean = sparkMlSettings.cls.useTopNBucketing
  val SPARK_ML_CLS_TOP_N_CATEGORIES: Int = sparkMlSettings.cls.topNCategories

  private val pipelineSettings: PipelineSettings = PipelineSettings(
    batchInvalidPath = envOrConfig("PIPELINE_BATCH_INVALID_PATH",
      Try(pipelineConfig.getString("batch_invalid_path")).getOrElse("batch/invalid")),
    batchDlqPath = envOrConfig("PIPELINE_BATCH_DLQ_PATH",
      Try(pipelineConfig.getString("batch_dlq_path")).getOrElse("batch/dlq")),
    batchLineagePath = envOrConfig("PIPELINE_BATCH_LINEAGE_PATH",
      Try(pipelineConfig.getString("batch_lineage_path")).getOrElse("lineage")),
    batchOffsetPath = envOrConfig("PIPELINE_BATCH_OFFSET_PATH",
      Try(pipelineConfig.getString("batch_offset_path")).getOrElse("batch/offsets")),
    streamingInvalidPath = envOrConfig("PIPELINE_STREAMING_INVALID_PATH",
      Try(pipelineConfig.getString("streaming_invalid_path")).getOrElse("streaming/invalid")),
    streamingDlqPath = envOrConfig("PIPELINE_STREAMING_DLQ_PATH",
      Try(pipelineConfig.getString("streaming_dlq_path")).getOrElse("streaming/dlq")),
    streamingLineagePath = envOrConfig("PIPELINE_STREAMING_LINEAGE_PATH",
      Try(pipelineConfig.getString("streaming_lineage_path")).getOrElse("streaming/lineage")),
    streamingDedupPath = envOrConfig("PIPELINE_STREAMING_DEDUP_PATH",
      Try(pipelineConfig.getString("streaming_dedup_path")).getOrElse("streaming/deduplication")),
    dlqShutdownWaitSeconds = envOrConfigInt("DLQ_SHUTDOWN_WAIT_SECONDS",
      Try(pipelineConfig.getInt("dlq_shutdown_wait_seconds")).getOrElse(5)),
    dedupPersistenceInterval = envOrConfigInt("DEDUP_PERSISTENCE_INTERVAL",
      Try(pipelineConfig.getInt("dedup_persistence_interval")).getOrElse(10)),
    dedupKeyColumns = envOrConfigSeq("DEDUP_KEY_COLUMNS",
      Try(pipelineConfig.getStringList("dedup_key_columns").asScala).getOrElse(Seq("event_time", "user_id", "product_id", "event_type")))
  )

  private val validationSettings: ValidationSettings = ValidationSettings(
    minPrice = envOrConfigDouble("VALIDATION_MIN_PRICE",
      Try(validationConfig.getDouble("min_price")).getOrElse(0.01)),
    maxPrice = envOrConfigDouble("VALIDATION_MAX_PRICE",
      Try(validationConfig.getDouble("max_price")).getOrElse(1000000.0)),
    validEventTypes = envOrConfigSeq("VALIDATION_VALID_EVENT_TYPES",
      Try(validationConfig.getStringList("valid_event_types").asScala).getOrElse(Seq("view", "cart", "purchase", "remove_from_cart")))
  )

  private val deduplicationSettings: DeduplicationSettings = DeduplicationSettings(
    maxCacheSize = envOrConfigInt("DEDUP_MAX_CACHE_SIZE",
      Try(deduplicationConfig.getInt("max_cache_size")).getOrElse(1000000)),
    loadHistoryHours = envOrConfigInt("DEDUP_LOAD_HISTORY_HOURS",
      Try(deduplicationConfig.getInt("load_history_hours")).getOrElse(24)),
    evictionBuffer = envOrConfigInt("DEDUP_EVICTION_BUFFER",
      Try(deduplicationConfig.getInt("eviction_buffer")).getOrElse(1000))
  )

  private val dlqSettings: DLQSettings = DLQSettings(
    queueSize = envOrConfigInt("DLQ_QUEUE_SIZE",
      Try(dlqConfig.getInt("queue_size")).getOrElse(10000)),
    offerTimeoutSeconds = envOrConfigInt("DLQ_OFFER_TIMEOUT_SECONDS",
      Try(dlqConfig.getInt("offer_timeout_seconds")).getOrElse(5)),
    pollTimeoutSeconds = envOrConfigInt("DLQ_POLL_TIMEOUT_SECONDS",
      Try(dlqConfig.getInt("poll_timeout_seconds")).getOrElse(1))
  )

  // Backwards compatible accessors
  val PIPELINE_BATCH_INVALID_PATH: String = s"$MINIO_BASE_PATH/${pipelineSettings.batchInvalidPath}"
  val PIPELINE_BATCH_DLQ_PATH: String = s"$MINIO_BASE_PATH/${pipelineSettings.batchDlqPath}"
  val PIPELINE_BATCH_LINEAGE_PATH: String = s"$MINIO_BASE_PATH/${pipelineSettings.batchLineagePath}"
  val PIPELINE_BATCH_OFFSET_PATH: String = s"$MINIO_BASE_PATH/${pipelineSettings.batchOffsetPath}"
  val PIPELINE_STREAMING_INVALID_PATH: String = s"$MINIO_BASE_PATH/${pipelineSettings.streamingInvalidPath}"
  val PIPELINE_STREAMING_DLQ_PATH: String = s"$MINIO_BASE_PATH/${pipelineSettings.streamingDlqPath}"
  val PIPELINE_STREAMING_LINEAGE_PATH: String = s"$MINIO_BASE_PATH/${pipelineSettings.streamingLineagePath}"
  val PIPELINE_STREAMING_DEDUP_PATH: String = s"$MINIO_BASE_PATH/${pipelineSettings.streamingDedupPath}"

  /** Convenience accessor that returns the full configuration as a single value. */
  val applicationConfig: ApplicationConfig =
    ApplicationConfig(
      kafkaSettings,
      minioSettings,
      clickhouseSettings,
      sparkSettings,
      sparkMlSettings,
      pipelineSettings,
      validationSettings,
      deduplicationSettings,
      dlqSettings
    )
}
