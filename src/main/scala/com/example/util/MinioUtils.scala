package com.example.util

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

/**
 * Utility object for MinIO/S3 operations with Delta Lake tables.
 * Provides functions for configuring MinIO, reading/writing Delta tables,
 * managing ML models, and optimizing Delta table performance.
 */
object MinioUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Validates that bucket name and path are not empty.
   * 
   * @param bucketName The bucket name to validate
   * @param path The path to validate
   * @throws IllegalArgumentException if bucket name or path is empty
   */
  private def validate(bucketName: String, path: String): Unit = {
    require(bucketName.nonEmpty, "Bucket name must be provided")
    require(path.nonEmpty, "Path must be provided")
  }

  /**
   * Configures Spark's Hadoop configuration for MinIO/S3 access.
   * 
   * This function sets up the S3A filesystem to work with MinIO by configuring
   * endpoint, credentials, SSL settings, and connection parameters.
   * 
   * @param spark The SparkSession to configure
   * @param endpoint The MinIO endpoint URL (e.g., "http://localhost:9000")
   * @param accessKey The MinIO access key
   * @param secretKey The MinIO secret key
   * @param pathStyleAccess Whether to use path-style access ("true" or "false")
   */
  def configureMinIO(
                      spark: SparkSession,
                      endpoint: String,
                      accessKey: String,
                      secretKey: String,
                      pathStyleAccess: String
                    ): Unit = {
    // Configure Hadoop filesystem for S3A/MinIO access
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    hadoopConf.set("fs.s3a.endpoint", endpoint)
    hadoopConf.set("fs.s3a.access.key", accessKey)
    hadoopConf.set("fs.s3a.secret.key", secretKey)
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoopConf.set("fs.s3a.path.style.access", pathStyleAccess)
    hadoopConf.set("fs.s3a.impl", classOf[S3AFileSystem].getName)
    hadoopConf.set("fs.s3a.connection.maximum", "15")
    hadoopConf.set("fs.s3a.attempts.maximum", "3")
    hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
  }

  /**
   * Checks if a MinIO bucket exists, creating it if it doesn't.
   * 
   * This function uses the AWS S3 client to interact with MinIO, verifying
   * bucket existence and creating it if necessary.
   * 
   * @param endpoint The MinIO endpoint URL
   * @param accessKey The MinIO access key
   * @param secretKey The MinIO secret key
   * @param bucket The bucket name to check/create
   * @return Success(()) if bucket exists or is created successfully,
   *         Failure(exception) if an error occurs
   */
  def checkBucketExists(
                         endpoint: String,
                         accessKey: String,
                         secretKey: String,
                         bucket: String
                       ): Try[Unit] = {
    Try {
      val credentials = new BasicAWSCredentials(accessKey, secretKey)
      val endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1")

      val s3Client = AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(endpointConfiguration)
        .withPathStyleAccessEnabled(true)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()

      // Check if bucket exists by attempting to list objects
      val bucketExists = Try(s3Client.listObjects(bucket)).isSuccess

      if (!bucketExists) {
        logger.info(s"Bucket '$bucket' not found. Creating it on $endpoint.")
        s3Client.createBucket(bucket)
      } else {
        logger.debug(s"Bucket '$bucket' already exists.")
      }

      s3Client.shutdown()
    }.recoverWith { case e: Exception =>
      logger.error(s"Failed to verify bucket '$bucket' on $endpoint", e)
      Failure(e)
    }
  }

  /**
   * Reads a Delta table from MinIO storage.
   * 
   * @param spark The SparkSession instance
   * @param bucketName The MinIO bucket name containing the Delta table
   * @param path The path within the bucket where the Delta table is stored
   * @return DataFrame containing the data from the Delta table
   * @throws Exception if the table cannot be read or path is invalid
   */
  def readDeltaTable(
                      spark: SparkSession,
                      bucketName: String,
                      path: String,
                    ): DataFrame = {
    validate(bucketName, path)
    val fullPath = s"s3a://$bucketName/$path"
    logger.info(s"Reading data from MinIO: $fullPath (format: delta)")

    try {
      val df = spark.read
        .format("delta")
        .load(fullPath)

      logger.info(s"Successfully read data from MinIO.")
      df
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to read data from MinIO: $fullPath", ex)
        throw ex
    }
  }

  /**
   * Writes a DataFrame to MinIO as a Delta table.
   * 
   * This function supports partitioning, repartitioning, and optional automatic optimization
   * of the Delta table after writing.
   * 
   * @param df The DataFrame to write
   * @param bucketName The MinIO bucket name where the Delta table will be stored
   * @param path The path within the bucket for storing the Delta table
   * @param saveMode The save mode: Append, Overwrite, ErrorIfExists, or Ignore (default: Append)
   * @param partitionColumns Optional sequence of column names to partition the table by
   * @param repartitionColumns Optional sequence of column names to repartition data before writing
   * @param autoOptimize If true, automatically optimizes the Delta table after writing (default: false)
   * @throws Exception if writing fails or path is invalid
   */
  def writeDeltaTable(
                       df: DataFrame,
                       bucketName: String,
                       path: String,
                       saveMode: SaveMode = SaveMode.Append,
                       partitionColumns: Option[Seq[String]] = None,
                       repartitionColumns: Option[Seq[String]] = None,
                       autoOptimize: Boolean = false
                     ): Unit = {
    validate(bucketName, path)

    val fullPath = s"s3a://$bucketName/$path"
    logger.info(s"Writing data to MinIO: $fullPath (format: delta, mode: $saveMode)")

    try {
      var payload = df

      // Repartition data if specified (useful for optimizing write performance)
      repartitionColumns match {
        case Some(cols) if cols.nonEmpty =>
          logger.info(s"Repartitioning data by: ${cols.mkString(", ")}")
          payload = payload.repartition(cols.map(col): _*)
        case _ =>
      }

      var writer = payload.write
        .format("delta")
        .mode(saveMode)

      // Apply partitioning if specified (improves query performance for filtered queries)
      partitionColumns match {
        case Some(cols) if cols.nonEmpty =>
          logger.info(s"Partitioning data by: ${cols.mkString(", ")}")
          writer = writer.partitionBy(cols: _*)
        case _ =>
      }

      writer.save(fullPath)

      if (autoOptimize) {
        logger.info("Auto-optimizing Delta table after write")
        optimizeDeltaTable(df.sparkSession, bucketName, path)
      }

      logger.info(s"Successfully wrote records to MinIO: $fullPath")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to write data to MinIO: $fullPath", ex)
        throw ex
    }
  }

  /**
   * Performs an upsert operation (merge) on a Delta table.
   * 
   * This function implements the Delta Lake merge operation, which updates existing rows
   * that match the merge condition and inserts new rows that don't match.
   * 
   * @param spark The SparkSession instance
   * @param bucketName The MinIO bucket name containing the target Delta table
   * @param path The path within the bucket where the target Delta table is stored
   * @param sourceDataFrame The source DataFrame containing data to merge
   * @param mergeCondition SQL condition for matching rows (e.g., "target.id = source.id")
   * @param updateCondition Optional SQL condition for when to update matched rows
   * @param insertCondition Optional SQL condition for when to insert non-matched rows
   * @throws Exception if merge fails or path is invalid
   */
  def mergeDeltaTable(
                       spark: SparkSession,
                       bucketName: String,
                       path: String,
                       sourceDataFrame: DataFrame,
                       mergeCondition: String,
                       updateCondition: Option[String] = None,
                       insertCondition: Option[String] = None
                     ): Unit = {
    validate(bucketName, path)

    val fullPath = s"s3a://$bucketName/$path"
    logger.info(s"Merging data into Delta table: $fullPath")

    try {
      val deltaTable = DeltaTable.forPath(spark, fullPath)

      val mergeBuilder = deltaTable
        .as("target")
        .merge(sourceDataFrame.as("source"), mergeCondition)

      // Configure update clause: update matched rows based on condition
      val updateClause = updateCondition match {
        case Some(condition) =>
          logger.info(s"Update condition: $condition")
          mergeBuilder.whenMatched(condition).updateAll()
        case None =>
          logger.info("Updating all matched rows")
          mergeBuilder.whenMatched().updateAll()
      }

      // Configure insert clause: insert non-matched rows based on condition
      val insertClause = insertCondition match {
        case Some(condition) =>
          logger.info(s"Insert condition: $condition")
          updateClause.whenNotMatched(condition).insertAll()
        case None =>
          logger.info("Inserting all non-matched rows")
          updateClause.whenNotMatched().insertAll()
      }

      insertClause.execute()

      logger.info(s"Successfully merged records into Delta table: $fullPath")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to merge Delta table: $fullPath", ex)
        throw ex
    }
  }

  /**
   * Optimizes a Delta table using bin packing or Z-ordering.
   * 
   * This private function is called internally by writeDeltaTable when autoOptimize is enabled.
   * Optimization improves query performance by compacting small files and organizing data.
   * 
   * @param spark The SparkSession instance
   * @param bucketName The MinIO bucket name containing the Delta table
   * @param path The path within the bucket where the Delta table is stored
   * @param zOrderColumns Optional sequence of columns for Z-ordering (alternative to bin packing)
   * @param whereClause Optional WHERE clause to optimize only a subset of data
   */
  private def optimizeDeltaTable(
                                  spark: SparkSession,
                                  bucketName: String,
                                  path: String,
                                  zOrderColumns: Option[Seq[String]] = None,
                                  whereClause: Option[String] = None
                                ): Unit = {
    validate(bucketName, path)

    val fullPath = s"s3a://$bucketName/$path"
    logger.info(s"Optimizing Delta table: $fullPath")

    try {
      val deltaTable = DeltaTable.forPath(spark, fullPath)

      var optimizer = deltaTable.optimize()

      if (whereClause.isDefined) {
        logger.info(s"Optimizing with WHERE clause: ${whereClause.get}")
        optimizer = optimizer.where(whereClause.get)
      }

      // Apply Z-ordering if specified, otherwise use bin packing compaction
      zOrderColumns match {
        case Some(cols) if cols.nonEmpty =>
          logger.info(s"Applying Z-ORDER BY: ${cols.mkString(", ")}")
          optimizer.executeZOrderBy(cols: _*)
        case _ =>
          logger.info("Executing bin packing optimization")
          optimizer.executeCompaction()
      }

      logger.info(s"Successfully optimized Delta table: $fullPath")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to optimize Delta table: $fullPath", ex)
        throw ex
    }
  }

  /**
   * Vacuums a Delta table to remove old files that are no longer needed.
   * 
   * Vacuuming removes files that are older than the retention period and are no longer
   * referenced by the Delta table's transaction log. This helps reduce storage costs.
   * 
   * @param spark The SparkSession instance
   * @param bucketName The MinIO bucket name containing the Delta table
   * @param path The path within the bucket where the Delta table is stored
   * @param retentionHours Files older than this many hours will be deleted (default: 168 = 7 days)
   * @throws Exception if vacuum fails or path is invalid
   */
  def vacuumDeltaTable(
                        spark: SparkSession,
                        bucketName: String,
                        path: String,
                        retentionHours: Int = 168,
                      ): Unit = {
    validate(bucketName, path)

    val fullPath = s"s3a://$bucketName/$path"

    try {
      val deltaTable = DeltaTable.forPath(spark, fullPath)
      logger.info(s"Deleting files older than $retentionHours hours")
      deltaTable.vacuum(retentionHours)
      logger.info(s"Successfully vacuumed Delta table: $fullPath")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to vacuum Delta table: $fullPath", ex)
        throw ex
    }
  }

  /**
   * Saves a Spark ML model to MinIO storage.
   * 
   * This generic function works with any ML model that implements MLWritable,
   * such as ALSModel, PipelineModel, or StringIndexerModel.
   * 
   * @param model The ML model to save (must implement MLWritable)
   * @param bucketName The MinIO bucket name where the model will be stored
   * @param path The path within the bucket for storing the model
   * @param overwrite If true, overwrites existing model at the path (default: true)
   * @tparam T The type of the ML model (must extend MLWritable)
   * @throws Exception if saving fails or path is invalid
   */
  def saveMLModel[T <: MLWritable](
                                    model: T,
                                    bucketName: String,
                                    path: String,
                                    overwrite: Boolean = true
                                  ): Unit = {
    validate(bucketName, path)

    val fullPath = s"s3a://$bucketName/$path"
    logger.info(s"Saving ML model to MinIO: $fullPath")

    try {
      val writer = if (overwrite) model.write.overwrite() else model.write
      writer.save(fullPath)
      logger.info(s"Successfully saved ML model to: $fullPath")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to save ML model to MinIO: $fullPath", ex)
        throw ex
    }
  }

  /**
   * Loads a Spark ML model from MinIO storage.
   * 
   * This generic function works with any ML model that implements MLReadable,
   * such as ALSModel, PipelineModel, or StringIndexerModel.
   * 
   * @param bucketName The MinIO bucket name containing the model
   * @param path The path within the bucket where the model is stored
   * @param loader The MLReadable loader for the specific model type
   * @tparam T The type of the ML model to load
   * @return The loaded ML model
   * @throws Exception if loading fails or path is invalid
   */
  def loadMLModel[T](
                      bucketName: String,
                      path: String,
                      loader: MLReadable[T]
                    ): T = {
    validate(bucketName, path)

    val fullPath = s"s3a://$bucketName/$path"
    logger.info(s"Loading ML model from MinIO: $fullPath")

    try {
      val model = loader.load(fullPath)
      logger.info(s"Successfully loaded ML model from: $fullPath")
      model
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to load ML model from MinIO: $fullPath", ex)
        throw ex
    }
  }


  /**
   * Saves a StringIndexerModel to MinIO storage.
   * 
   * This is a convenience wrapper around saveMLModel specifically for StringIndexerModel.
   * 
   * @param model The StringIndexerModel to save
   * @param bucketName The MinIO bucket name where the model will be stored
   * @param path The path within the bucket for storing the model
   * @param overwrite If true, overwrites existing model at the path (default: true)
   */
  def saveIndexerModel(
                        model: StringIndexerModel,
                        bucketName: String,
                        path: String,
                        overwrite: Boolean = true
                      ): Unit = {
    val labelCount = model.labelsArray.headOption.map(_.length).getOrElse(0)
    logger.info(s"Saving StringIndexer model with $labelCount labels")
    saveMLModel(model, bucketName, path, overwrite)
  }

  /**
   * Loads a StringIndexerModel from MinIO storage.
   * 
   * This is a convenience wrapper around loadMLModel specifically for StringIndexerModel.
   * 
   * @param bucketName The MinIO bucket name containing the model
   * @param path The path within the bucket where the model is stored
   * @return The loaded StringIndexerModel
   */
  def loadIndexerModel(
                        bucketName: String,
                        path: String
                      ): StringIndexerModel = {
    loadMLModel(bucketName, path, StringIndexerModel)
  }

  /**
   * Loads an ALSModel from MinIO storage.
   * 
   * This is a convenience wrapper around loadMLModel specifically for ALSModel.
   * 
   * @param bucketName The MinIO bucket name containing the model
   * @param path The path within the bucket where the model is stored
   * @return The loaded ALSModel
   */
  def loadALSModel(
                    bucketName: String,
                    path: String
                  ): ALSModel = {
    loadMLModel(bucketName, path, ALSModel)
  }

  /**
   * Loads a PipelineModel (classification model) from MinIO storage.
   * 
   * This is a convenience wrapper around loadMLModel specifically for PipelineModel.
   * 
   * @param bucketName The MinIO bucket name containing the model
   * @param path The path within the bucket where the model is stored
   * @return The loaded PipelineModel
   */
  def loadClassificationModel(
                               bucketName: String,
                               path: String
                             ): PipelineModel = {
    loadMLModel(bucketName, path, PipelineModel)
  }
}
