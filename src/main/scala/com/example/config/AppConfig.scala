package com.example.config

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Centralized application configuration management.
 * 
 * This object loads configuration from environment variables and configuration files,
 * with environment variables taking precedence. Supports both development and production
 * environments through the ENV_JOB_RUN environment variable.
 * 
 * Configuration is loaded from:
 * - Environment variables (highest priority)
 * - application.prod.conf (when ENV_JOB_RUN=prod)
 * - application.dev.conf (default)
 * 
 * Provides type-safe configuration through case classes and maintains backward compatibility
 * through legacy field accessors.
 */
object AppConfig {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Kafka connection and topic configuration.
   * 
   * @param bootstrapServers Comma-separated list of Kafka broker addresses
   * @param batchTopic Kafka topic name for batch processing
   * @param streamTopic Kafka topic name for streaming processing
   * @param checkpointLocation Path for Spark checkpoint directory
   * @param startingOffsets Starting offset strategy (e.g., "earliest", "latest", or JSON)
   */
  final case class KafkaSettings(
    bootstrapServers: String,
    batchTopic: String,
    streamTopic: String,
    checkpointLocation: String,
    startingOffsets: String,
  )

  /**
   * MinIO/S3 storage configuration.
   * 
   * @param endpoint MinIO server endpoint URL
   * @param accessKey MinIO access key for authentication
   * @param secretKey MinIO secret key for authentication
   * @param bucketName Default bucket name for data storage
   * @param basePath Base path prefix within the bucket
   * @param pathStyleAccess Whether to use path-style access ("true" or "false")
   */
  final case class MinioSettings(
    endpoint: String,
    accessKey: String,
    secretKey: String,
    bucketName: String,
    basePath: String,
    pathStyleAccess: String
  )

  /**
   * Google Cloud Storage (GCS) configuration.
   * 
   * @param projectId GCP project ID
   * @param bucketName GCS bucket name for data storage
   * @param basePath Base path prefix within the bucket
   * @param credentialsPath Optional path to service account JSON key file
   */
  final case class GcsSettings(
    projectId: String,
    bucketName: String,
    basePath: String,
    credentialsPath: Option[String] = None
  )

  /**
   * ClickHouse database connection configuration.
   * 
   * @param url JDBC connection URL for ClickHouse
   * @param user Database username
   * @param password Database password
   * @param table Default table name for operations
   * @param batchSize Batch size for bulk insert operations
   * @param maxConnections Maximum number of connections in the pool (default: 10)
   * @param connectionTimeoutMs Connection timeout in milliseconds (default: 30000)
   * @param socketTimeoutMs Socket timeout in milliseconds (default: 30000)
   */
  final case class ClickhouseSettings(
    url: String,
    user: String,
    password: String,
    table: String,
    batchSize: Int,
    maxConnections: Int = 10,
    connectionTimeoutMs: Int = 30000,
    socketTimeoutMs: Int = 30000
  )

  /**
   * Spark application configuration.
   * 
   * @param master Spark master URL (e.g., "local[*]", "yarn", "spark://host:port")
   * @param shufflePartitions Number of partitions for shuffle operations
   * @param timestampPattern Pattern for parsing timestamp strings
   * @param timezone Timezone for timestamp operations
   * @param watermarkDuration Watermark duration for streaming (e.g., "5 minutes")
   */
  final case class SparkSettings(
    master: String,
    shufflePartitions: Int,
    timestampPattern: String,
    timezone: String,
    watermarkDuration: String
  )

  /**
   * Data pipeline path configuration for Medallion Architecture zones.
   * 
   * @param rawZonePath Path for Raw Zone (Bronze) - stores raw ingested data
   * @param workingZonePath Path for Working Zone (Silver) - stores cleaned/validated data
   * @param goldZonePath Path for Gold Zone - stores aggregated/curated data
   * @param dedupKeyColumns Column names used for deduplication (default: event_time, user_id, product_id, event_type)
   */
  final case class PipelineSettings(
    rawZonePath: String = "raw_zone",
    workingZonePath: String = "working_zone",
    goldZonePath: String = "gold_zone",
    dedupKeyColumns: Seq[String] = Seq("event_time", "user_id", "product_id", "event_type")
  )

  /**
   * Data validation rules configuration.
   * 
   * @param minPrice Minimum valid price value (default: 0.01)
   * @param maxPrice Maximum valid price value (default: 1000000.0)
   * @param validEventTypes List of valid event type values (default: view, cart, purchase, remove_from_cart)
   */
  final case class ValidationSettings(
    minPrice: Double = 0.01,
    maxPrice: Double = 1000000.0,
    validEventTypes: Seq[String] = Seq("view", "cart", "purchase", "remove_from_cart")
  )

  /**
   * Dead Letter Queue (DLQ) configuration.
   * 
   * @param queueSize Maximum size of the DLQ buffer (default: 10000)
   * @param offerTimeoutSeconds Timeout for offering items to the queue in seconds (default: 5)
   * @param pollTimeoutSeconds Timeout for polling items from the queue in seconds (default: 1)
   */
  final case class DLQSettings(
    queueSize: Int = 10000,
    offerTimeoutSeconds: Int = 5,
    pollTimeoutSeconds: Int = 1
  )

  /**
   * Event weight configuration for ALS (Alternating Least Squares) recommendation model.
   * 
   * Different event types are weighted differently to reflect their importance
   * in user-item interactions for collaborative filtering.
   * 
   * @param view Weight for view events
   * @param cart Weight for cart events
   * @param removeFromCart Weight for remove_from_cart events (typically negative)
   * @param purchase Weight for purchase events (typically highest)
   * @param viewCap Maximum weight cap for view events to prevent over-weighting
   */
  final case class ALSEventWeight(
    view: Double,
    cart: Double,
    removeFromCart: Double,
    purchase: Double,
    viewCap: Double,
  )

  /**
   * ALS (Alternating Least Squares) recommendation model configuration.
   * 
   * @param base_path Base path in MinIO for storing ALS model artifacts
   * @param rank Number of latent factors in the ALS model
   * @param iter Maximum number of iterations for ALS training
   * @param regParam Regularization parameter to prevent overfitting
   * @param alpha Implicit feedback confidence scaling parameter
   * @param K Number of top recommendations to evaluate (top-K)
   * @param blockFactor Block size for ALS computation (for performance tuning)
   * @param eventWeight Event weight configuration for different interaction types
   */
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

  /**
   * Event weight configuration for classification model.
   * 
   * @param view Weight for view events
   * @param cart Weight for cart events
   * @param removeFromCart Weight for remove_from_cart events
   * @param purchase Weight for purchase events
   */
  final case class ClsEventWeight(
    view: Double,
    cart: Double,
    removeFromCart: Double,
    purchase: Double
  )

  /**
   * Classification model (Random Forest) configuration.
   * 
   * @param base_path Base path in MinIO for storing classification model artifacts
   * @param eventWeight Event weight configuration for different interaction types
   * @param trees Number of trees in the Random Forest ensemble
   * @param maxDepth Maximum depth of each decision tree
   * @param minInstancesPerNode Minimum number of instances required at a leaf node
   * @param maxBins Maximum number of bins for discretizing continuous features
   * @param subsamplingRate Fraction of training data used for each tree (bootstrap sampling)
   * @param featureSubsetStrategy Strategy for selecting features at each split
   * @param useTopNBucketing Whether to use top-N bucketing for categorical features
   * @param topNCategories Number of top categories to keep when bucketing is enabled
   */
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

  /**
   * Spark ML (Machine Learning) configuration container.
   * 
   * @param timeDecayFactor Time decay factor for weighting recent events more heavily
   * @param als ALS recommendation model configuration
   * @param cls Classification model configuration
   */
  final case class SparkMLConfig(
    timeDecayFactor: Double,
    als: ALSConfig,
    cls: ClsConfig
  )

  /**
   * Complete application configuration container.
   * 
   * Aggregates all configuration settings into a single type-safe structure.
   * 
   * @param kafka Kafka configuration
   * @param minio MinIO storage configuration
   * @param gcs Optional GCS storage configuration (alternative to MinIO)
   * @param clickhouse ClickHouse database configuration
   * @param spark Spark application configuration
   * @param ml Machine learning model configurations
   * @param pipeline Pipeline path configurations (default: PipelineSettings())
   * @param validation Data validation rules (default: ValidationSettings())
   * @param dlq Dead letter queue settings (default: DLQSettings())
   */
  final case class ApplicationConfig(
    kafka: KafkaSettings,
    minio: MinioSettings,
    gcs: Option[GcsSettings] = None,
    clickhouse: ClickhouseSettings,
    spark: SparkSettings,
    ml: SparkMLConfig,
    pipeline: PipelineSettings = PipelineSettings(),
    validation: ValidationSettings = ValidationSettings(),
    dlq: DLQSettings = DLQSettings()
  )

  // Determine environment (prod or dev) from environment variable
  private val envStats: String = sys.env.getOrElse("ENV_JOB_RUN", "env")
  logger.info(s"Loading configuration for environment: $envStats")

  /**
   * Loads configuration file based on environment.
   * 
   * Loads application.prod.conf for production environment (ENV_JOB_RUN=prod),
   * otherwise loads application.dev.conf for development.
   */
  val config: Config = {
    val fileName = if (envStats == "prod") "application.prod.conf" else "application.dev.conf"
    Try(ConfigFactory.load(fileName)).recover {
      case ex: Exception =>
        logger.error(s"Failed to load $fileName", ex)
        throw ex
    }.get
  }

  // Extract configuration sections from the main config
  private val kafkaConfig = config.getConfig("kafka")
  private val minioConfig = config.getConfig("minio")
  private val gcsConfig = Try(config.getConfig("gcs")).toOption
  private val clickhouseConfig = config.getConfig("clickhouse")
  private val sparkConfig = config.getConfig("spark")
  private val sparkMlConfig = config.getConfig("ml")
  private val alsConfig = sparkMlConfig.getConfig("als")
  private val alsEventWeightConfig = alsConfig.getConfig("event_weights")
  private val clsConfig = sparkMlConfig.getConfig("classification")
  private val clsEventWeightConfig = clsConfig.getConfig("weights")

  // Optional configuration sections (use empty config if not present)
  private val pipelineConfig = Try(config.getConfig("pipeline")).getOrElse(ConfigFactory.empty())
  private val validationConfig = Try(config.getConfig("validation")).getOrElse(ConfigFactory.empty())
  private val dlqConfig = Try(config.getConfig("dlq")).getOrElse(ConfigFactory.empty())

  /**
   * Validates that a configuration value is not null or empty.
   * 
   * @param value The configuration value to validate
   * @param key The configuration key name (for error messages)
   * @return Trimmed value if valid
   * @throws IllegalArgumentException if value is null or empty
   */
  private def requireNonEmpty(value: String, key: String): String = {
    if (value == null || value.trim.isEmpty) {
      throw new IllegalArgumentException(s"Configuration value for '$key' must not be empty")
    }
    value.trim
  }

  /**
   * Resolves configuration value from environment variable or config file.
   * 
   * Environment variables take precedence over configuration file values.
   * The resolved value must be non-empty.
   * 
   * @param envKey Environment variable key to check first
   * @param defaultValue Lazy-evaluated default value from config file
   * @return Resolved configuration value (trimmed)
   * @throws IllegalArgumentException if resolved value is empty
   */
  private def envOrConfig(envKey: String, defaultValue: => String): String = {
    val resolved = sys.env.get(envKey).filter(_.nonEmpty).getOrElse(defaultValue)
    requireNonEmpty(resolved, envKey)
  }

  /**
   * Resolves integer configuration value from environment variable or config file.
   * 
   * @param envKey Environment variable key to check first
   * @param defaultValue Default value from config file if env var not set or invalid
   * @return Resolved integer value
   */
  private def envOrConfigInt(envKey: String, defaultValue: => Int): Int = {
    sys.env.get(envKey).flatMap(v => Try(v.toInt).toOption)
      .getOrElse(defaultValue)
  }

  /**
   * Resolves double configuration value from environment variable or config file.
   * 
   * @param envKey Environment variable key to check first
   * @param defaultValue Default value from config file if env var not set or invalid
   * @return Resolved double value
   */
  private def envOrConfigDouble(envKey: String, defaultValue: => Double): Double = {
    sys.env.get(envKey).flatMap(v => Try(v.toDouble).toOption)
      .getOrElse(defaultValue)
  }

  /**
   * Resolves sequence of strings from environment variable or config file.
   * 
   * Environment variable should be comma-separated values. If not set, uses
   * the default sequence from config file.
   * 
   * @param envKey Environment variable key to check first
   * @param defaultValue Default sequence from config file
   * @return Resolved sequence of strings
   */
  private def envOrConfigSeq(envKey: String, defaultValue: => Seq[String]): Seq[String] = {
    sys.env.get(envKey).map(_.split(",").map(_.trim).toSeq)
      .getOrElse(defaultValue)
  }

  /**
   * Resolves boolean configuration value from environment variable or config file.
   * 
   * @param envKey Environment variable key to check first
   * @param defaultValue Default value from config file if env var not set or invalid
   * @return Resolved boolean value
   */
  private def envOrConfigBoolean(envKey: String, defaultValue: => Boolean): Boolean = {
    sys.env.get(envKey).flatMap(v => Try(v.toBoolean).toOption)
      .getOrElse(defaultValue)
  }

  // Build configuration objects from environment variables and config files
  private val kafkaSettings: KafkaSettings = KafkaSettings(
    bootstrapServers = envOrConfig("KAFKA_BOOTSTRAP_SERVERS", kafkaConfig.getString("bootstrap_servers")),
    batchTopic = envOrConfig("KAFKA_TOPIC", kafkaConfig.getString("batch_topic")),
    streamTopic = envOrConfig("KAFKA_STREAM_TOPIC", kafkaConfig.getString("stream_topic")),
    checkpointLocation = envOrConfig("KAFKA_CHECKPOINT_LOCATION", kafkaConfig.getString("checkpoint_location")),
    startingOffsets = envOrConfig("KAFKA_STARTING_OFFSETS", kafkaConfig.getString("starting_offsets"))
  )

  private val minioSettings: MinioSettings = MinioSettings(
    endpoint = envOrConfig("MINIO_ENDPOINT", minioConfig.getString("endpoint")),
    accessKey = envOrConfig("MINIO_ACCESS_KEY",
      sys.env.getOrElse("MINIO_ROOT_USER", minioConfig.getString("access_key"))
    ),
    secretKey = envOrConfig("MINIO_SECRET_KEY",
      sys.env.getOrElse("MINIO_ROOT_PASSWORD", minioConfig.getString("secret_key"))
    ),
    bucketName = envOrConfig("MINIO_BUCKET_NAME", minioConfig.getString("bucket_name")),
    basePath = envOrConfig("MINIO_BASE_PATH", minioConfig.getString("base_path")),
    pathStyleAccess = envOrConfig("MINIO_PATH_STYLE_ACCESS", minioConfig.getString("path_style_access"))
  )

  // GCS settings (optional, alternative to MinIO)
  private val gcsSettings: Option[GcsSettings] = gcsConfig.map { cfg =>
    GcsSettings(
      projectId = envOrConfig("GCS_PROJECT_ID", cfg.getString("project_id")),
      bucketName = envOrConfig("GCS_BUCKET_NAME", cfg.getString("bucket_name")),
      basePath = envOrConfig("GCS_BASE_PATH", cfg.getString("base_path")),
      credentialsPath = sys.env.get("GOOGLE_APPLICATION_CREDENTIALS")
        .orElse(Try(cfg.getString("credentials_path")).toOption)
    )
  }

  /**
   * ClickHouse settings (publicly accessible for connection initialization).
   * 
   * This is exposed as a public value to allow other modules to initialize
   * ClickHouse connections using these settings.
   */
  val clickhouseSettings: ClickhouseSettings = ClickhouseSettings(
    url = envOrConfig("CLICKHOUSE_JDBC_URL", clickhouseConfig.getString("jdbc_url")),
    user = envOrConfig("CLICKHOUSE_USER", clickhouseConfig.getString("user")),
    password = envOrConfig("CLICKHOUSE_PASSWORD", clickhouseConfig.getString("password")),
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

  /**
   * Validates classification model configuration constraints.
   * 
   * When top-N bucketing is enabled, topNCategories + 1 must be <= maxBins
   * to accommodate the "other" category bucket in addition to the top N categories.
   */
  if (sparkMlSettings.cls.useTopNBucketing) {
    require(sparkMlSettings.cls.topNCategories + 1 <= sparkMlSettings.cls.maxBins,
      s"topNCategories (${sparkMlSettings.cls.topNCategories}) + 1 must be <= maxBins (${sparkMlSettings.cls.maxBins}) to accommodate 'other' bucket")
  }

  /**
   * Backward compatibility: Legacy field accessors.
   * 
   * These fields maintain backward compatibility with code that directly
   * accesses AppConfig fields instead of using the structured configuration objects.
   * New code should prefer using the case class instances (e.g., kafkaSettings).
   */
  
  // Kafka settings
  val KAFKA_BOOTSTRAP_SERVERS: String = kafkaSettings.bootstrapServers
  val KAFKA_BATCH_TOPIC: String = kafkaSettings.batchTopic
  val KAFKA_STREAM_TOPIC: String = kafkaSettings.streamTopic
  val KAFKA_CHECKPOINT_LOCATION: String = kafkaSettings.checkpointLocation
  val KAFKA_STARTING_OFFSETS: String = kafkaSettings.startingOffsets

  val MINIO_ENDPOINT: String = minioSettings.endpoint
  val MINIO_ACCESS_KEY: String = minioSettings.accessKey
  val MINIO_SECRET_KEY: String = minioSettings.secretKey
  val MINIO_BUCKET_NAME: String = minioSettings.bucketName
  val MINIO_BASE_PATH: String = minioSettings.basePath
  val MINIO_PATH_STYLE_ACCESS: String = minioSettings.pathStyleAccess

  val CLICKHOUSE_URL: String = clickhouseSettings.url
  val CLICKHOUSE_USER: String = clickhouseSettings.user
  val CLICKHOUSE_PASSWORD: String = clickhouseSettings.password
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
    rawZonePath = envOrConfig("PIPELINE_RAW_ZONE_PATH",
      Try(pipelineConfig.getString("raw_zone_path")).getOrElse("raw_zone")),
    workingZonePath = envOrConfig("PIPELINE_WORKING_ZONE_PATH",
      Try(pipelineConfig.getString("working_zone_path")).getOrElse("working_zone")),
    goldZonePath = envOrConfig("PIPELINE_GOLD_ZONE_PATH",
      Try(pipelineConfig.getString("gold_zone_path")).getOrElse("gold_zone")),
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

  private val dlqSettings: DLQSettings = DLQSettings(
    queueSize = envOrConfigInt("DLQ_QUEUE_SIZE",
      Try(dlqConfig.getInt("queue_size")).getOrElse(10000)),
    offerTimeoutSeconds = envOrConfigInt("DLQ_OFFER_TIMEOUT_SECONDS",
      Try(dlqConfig.getInt("offer_timeout_seconds")).getOrElse(5)),
    pollTimeoutSeconds = envOrConfigInt("DLQ_POLL_TIMEOUT_SECONDS",
      Try(dlqConfig.getInt("poll_timeout_seconds")).getOrElse(1))
  )

  // Medallion Architecture Zone paths (prefixed with MinIO base path)
  val RAW_ZONE_PATH: String = s"$MINIO_BASE_PATH/${pipelineSettings.rawZonePath}"
  val WORKING_ZONE_PATH: String = s"$MINIO_BASE_PATH/${pipelineSettings.workingZonePath}"
  val GOLD_ZONE_PATH: String = s"$MINIO_BASE_PATH/${pipelineSettings.goldZonePath}"

  /**
   * Complete application configuration object.
   * 
   * This is the main entry point for accessing all configuration settings
   * in a type-safe, structured manner. Use this instead of individual fields
   * for better maintainability and type safety.
   */
  val applicationConfig: ApplicationConfig =
    ApplicationConfig(
      kafkaSettings,
      minioSettings,
      gcsSettings,
      clickhouseSettings,
      sparkSettings,
      sparkMlSettings,
      pipelineSettings,
      validationSettings,
      dlqSettings
    )
}
