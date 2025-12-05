package com.example.util

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{bucket, col}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

object MinioUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  def configureMinIO(
                      spark: SparkSession,
                      endpoint: String,
                      accessKey: String,
                      secretKey: String,
                      pathStyleAccess: String
                    ): Unit = {
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

  def readDeltaTableWithTimeTravel(
                                    spark: SparkSession,
                                    bucketName: String,
                                    path: String,
                                    version: Option[Long] = None,
                                    timestamp: Option[String] = None
                                  ): DataFrame = {
    validate(bucketName, path)

    val fullPath = s"s3a://$bucketName/$path"

    try {
      val baseReader = spark.read.format("delta")

      val reader = (version, timestamp) match {
        case (Some(v), _) =>
          logger.info(s"Reading Delta table at version $v: $fullPath")
          baseReader.option("versionAsOf", v)

        case (None, Some(ts)) =>
          logger.info(s"Reading Delta table at timestamp $ts: $fullPath")
          baseReader.option("timestampAsOf", ts)

        case (None, None) =>
          logger.info(s"Reading latest version of Delta table: $fullPath")
          baseReader
      }

      val df = reader.load(fullPath)
      logger.info(s"Successfully read Delta table.")
      df
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to read Delta table with time travel: $fullPath", ex)
        throw ex
    }
  }

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

      repartitionColumns match {
        case Some(cols) if cols.nonEmpty =>
          logger.info(s"Repartitioning data by: ${cols.mkString(", ")}")
          payload = payload.repartition(cols.map(col): _*)
        case _ =>
      }

      var writer = payload.write
        .format("delta")
        .mode(saveMode)

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

      var mergeBuilder = deltaTable
        .as("target")
        .merge(sourceDataFrame.as("source"), mergeCondition)

      // Update clause
      val updateClause = updateCondition match {
        case Some(condition) =>
          logger.info(s"Update condition: $condition")
          mergeBuilder.whenMatched(condition).updateAll()
        case None =>
          logger.info("Updating all matched rows")
          mergeBuilder.whenMatched().updateAll()
      }

      // Insert clause
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

  private def validate(bucketName: String, path: String): Unit = {
    require(bucketName.nonEmpty, "Bucket name must be provided")
    require(path.nonEmpty, "Path must be provided")
  }
}
