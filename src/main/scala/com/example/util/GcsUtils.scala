package com.example.util

import com.google.cloud.storage.{BucketInfo, Storage, StorageOptions}
import io.delta.tables.DeltaTable
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

/**
 * Utility object for Google Cloud Storage (GCS) operations with Delta Lake tables.
 * Provides functions for configuring GCS, reading/writing Delta tables,
 * managing ML models, and optimizing Delta table performance.
 * 
 * This is a GCS alternative to MinioUtils for cloud-native deployments.
 */
object GcsUtils {
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
   * Configures Spark's Hadoop configuration for GCS access.
   * 
   * This function sets up the Google Cloud Storage filesystem to work with Spark
   * by configuring authentication, project ID, and connection parameters.
   * 
   * @param spark The SparkSession to configure
   * @param projectId The GCP project ID
   * @param credentialsPath Path to the service account JSON key file (optional if using Workload Identity)
   */
  def configureGcs(
                    spark: SparkSession,
                    projectId: String,
                    credentialsPath: Option[String] = None
                  ): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Configure Google Cloud Storage filesystem
    hadoopConf.set("fs.gs.impl", classOf[GoogleHadoopFileSystem].getName)
    hadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoopConf.set("fs.gs.project.id", projectId)
    
    // Set credentials path if provided (for non-Workload Identity setups)
    credentialsPath.foreach { path =>
      hadoopConf.set("google.cloud.auth.service.account.json.keyfile", path)
      hadoopConf.set("fs.gs.auth.service.account.json.keyfile", path)
    }

    // Performance and reliability settings
    hadoopConf.set("fs.gs.block.size", "134217728") // 128MB block size
    hadoopConf.set("fs.gs.inputstream.buffer.size", "8388608") // 8MB buffer
    hadoopConf.set("fs.gs.outputstream.buffer.size", "8388608") // 8MB buffer
    hadoopConf.set("fs.gs.http.max.retry", "10")
    hadoopConf.set("fs.gs.http.connect-timeout", "20000")
    hadoopConf.set("fs.gs.http.read-timeout", "20000")
    
    logger.info(s"Configured GCS access for project: $projectId")
  }

  /**
   * Checks if a GCS bucket exists, creating it if it doesn't.
   * 
   * This function uses the Google Cloud Storage client library to interact with GCS,
   * verifying bucket existence and creating it if necessary.
   * 
   * @param projectId The GCP project ID
   * @param bucket The bucket name to check/create
   * @param credentialsPath Optional path to service account JSON key file
   * @return Success(()) if bucket exists or is created successfully,
   *         Failure(exception) if an error occurs
   */
  def checkBucketExists(
                         projectId: String,
                         bucket: String,
                         credentialsPath: Option[String] = None
                       ): Try[Unit] = {
    Try {
      val storageBuilder = StorageOptions.newBuilder().setProjectId(projectId)
      
      // Set credentials if provided
      credentialsPath.foreach { path =>
        import com.google.auth.oauth2.GoogleCredentials
        import java.nio.file.{Files, Paths}
        val credentials = GoogleCredentials.fromStream(Files.newInputStream(Paths.get(path)))
        storageBuilder.setCredentials(credentials)
      }
      
      val storage: Storage = storageBuilder.build().getService

      // Check if bucket exists by attempting to get it
      // storage.get() throws StorageException if bucket doesn't exist
      val bucketExists = Try(storage.get(bucket)).isSuccess

      if (!bucketExists) {
        logger.info(s"Bucket '$bucket' not found. Creating it in project $projectId.")
        storage.create(BucketInfo.of(bucket))
        ()
      } else {
        logger.debug(s"Bucket '$bucket' already exists.")
        ()
      }
    }.recoverWith { case e: Exception =>
      logger.error(s"Failed to verify bucket '$bucket' in project $projectId", e)
      Failure(e)
    }
  }

  /**
   * Reads a Delta table from GCS storage.
   * 
   * @param spark The SparkSession instance
   * @param bucketName The GCS bucket name containing the Delta table
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
    val fullPath = s"gs://$bucketName/$path"
    logger.info(s"Reading data from GCS: $fullPath (format: delta)")

    try {
      val df = spark.read
        .format("delta")
        .load(fullPath)

      logger.info(s"Successfully read data from GCS.")
      df
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to read data from GCS: $fullPath", ex)
        throw ex
    }
  }

  /**
   * Writes a DataFrame to GCS as a Delta table.
   * 
   * This function supports partitioning, repartitioning, and optional automatic optimization
   * of the Delta table after writing.
   * 
   * @param df The DataFrame to write
   * @param bucketName The GCS bucket name where the Delta table will be stored
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

    val fullPath = s"gs://$bucketName/$path"
    logger.info(s"Writing data to GCS: $fullPath (format: delta, mode: $saveMode)")

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

      logger.info(s"Successfully wrote records to GCS: $fullPath")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to write data to GCS: $fullPath", ex)
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
   * @param bucketName The GCS bucket name containing the target Delta table
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

    val fullPath = s"gs://$bucketName/$path"
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
   * @param bucketName The GCS bucket name containing the Delta table
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

    val fullPath = s"gs://$bucketName/$path"
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
   * @param bucketName The GCS bucket name containing the Delta table
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

    val fullPath = s"gs://$bucketName/$path"

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
   * Saves a Spark ML model to GCS storage.
   * 
   * This generic function works with any ML model that implements MLWritable,
   * such as ALSModel, PipelineModel, or StringIndexerModel.
   * 
   * @param model The ML model to save (must implement MLWritable)
   * @param bucketName The GCS bucket name where the model will be stored
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

    val fullPath = s"gs://$bucketName/$path"
    logger.info(s"Saving ML model to GCS: $fullPath")

    try {
      val writer = if (overwrite) model.write.overwrite() else model.write
      writer.save(fullPath)
      logger.info(s"Successfully saved ML model to: $fullPath")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to save ML model to GCS: $fullPath", ex)
        throw ex
    }
  }

  /**
   * Loads a Spark ML model from GCS storage.
   * 
   * This generic function works with any ML model that implements MLReadable,
   * such as ALSModel, PipelineModel, or StringIndexerModel.
   * 
   * @param bucketName The GCS bucket name containing the model
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

    val fullPath = s"gs://$bucketName/$path"
    logger.info(s"Loading ML model from GCS: $fullPath")

    try {
      val model = loader.load(fullPath)
      logger.info(s"Successfully loaded ML model from: $fullPath")
      model
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to load ML model from GCS: $fullPath", ex)
        throw ex
    }
  }


  /**
   * Saves a StringIndexerModel to GCS storage.
   * 
   * This is a convenience wrapper around saveMLModel specifically for StringIndexerModel.
   * 
   * @param model The StringIndexerModel to save
   * @param bucketName The GCS bucket name where the model will be stored
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
   * Loads a StringIndexerModel from GCS storage.
   * 
   * This is a convenience wrapper around loadMLModel specifically for StringIndexerModel.
   * 
   * @param bucketName The GCS bucket name containing the model
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
   * Loads an ALSModel from GCS storage.
   * 
   * This is a convenience wrapper around loadMLModel specifically for ALSModel.
   * 
   * @param bucketName The GCS bucket name containing the model
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
   * Loads a PipelineModel (classification model) from GCS storage.
   * 
   * This is a convenience wrapper around loadMLModel specifically for PipelineModel.
   * 
   * @param bucketName The GCS bucket name containing the model
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
