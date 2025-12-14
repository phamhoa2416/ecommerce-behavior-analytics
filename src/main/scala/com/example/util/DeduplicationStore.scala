package com.example.util

import com.example.config.AppConfig
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, concat_ws, current_timestamp, expr}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object DeduplicationStore {
  private val logger = LoggerFactory.getLogger(getClass)

  private val seenKeys: mutable.Set[String] = mutable.Set.empty[String]
  private var maxCacheSize: Int = AppConfig.applicationConfig.deduplication.maxCacheSize
  private var persistencePath: Option[String] = None
  private var loadHistoryHours: Int = AppConfig.applicationConfig.deduplication.loadHistoryHours
  private var evictionBuffer: Int = AppConfig.applicationConfig.deduplication.evictionBuffer


  def initialize(
                  spark: SparkSession,
                  bucketName: String,
                  dedupPath: String,
                  maxCacheSize: Int = AppConfig.applicationConfig.deduplication.maxCacheSize,
                  loadFromPersistence: Boolean = true
                ): Try[Unit] = {
    try {
      this.maxCacheSize = maxCacheSize
      this.loadHistoryHours = AppConfig.applicationConfig.deduplication.loadHistoryHours
      this.evictionBuffer = AppConfig.applicationConfig.deduplication.evictionBuffer
      this.persistencePath = Some(s"s3a://$bucketName/$dedupPath")

      if (loadFromPersistence) {
        loadFromDelta(spark, bucketName, dedupPath)
      }

      logger.info(s"Deduplication store initialized (cacheSize=$maxCacheSize, persistencePath=$dedupPath)")
      Success(())
    } catch {
      case ex: Exception =>
        logger.error("Failed to initialize deduplication store", ex)
        Failure(ex)
    }
  }

  private def loadFromDelta(
                             spark: SparkSession,
                             bucketName: String,
                             dedupPath: String
                           ): Unit = {
    try {
      val fullPath = s"s3a://$bucketName/$dedupPath"
      val deltaTableExists = scala.util.Try {
        io.delta.tables.DeltaTable.forPath(spark, fullPath)
      }.isSuccess

      if (deltaTableExists) {
        val df = spark.read
          .format("delta")
          .load(fullPath)
          .filter(col("dedup_timestamp") >= current_timestamp() - expr(s"INTERVAL $loadHistoryHours HOURS"))
          .select("dedup_key")
          .distinct()

        val keys = df.collect().map(_.getAs[String]("dedup_key"))
        seenKeys ++= keys
        logger.info(s"Loaded ${keys.length} deduplication keys from persistence")
      } else {
        logger.info("No existing deduplication store found, starting fresh")
      }
    } catch {
      case ex: Exception =>
        logger.warn("Failed to load deduplication keys from persistence, starting fresh", ex)
    }
  }

  def isSeen(key: String): Boolean = {
    seenKeys.contains(key)
  }

  def markSeen(key: String): Unit = {
    if (seenKeys.size >= maxCacheSize) {
      val keysToRemove = seenKeys.take(seenKeys.size - maxCacheSize + evictionBuffer)
      seenKeys --= keysToRemove
    }
    seenKeys += key
  }

  def markSeenBatch(keys: Seq[String]): Unit = {
    keys.foreach(markSeen)
  }

  def filterDuplicates(df: DataFrame, keyColumns: Seq[String] = Seq("event_time", "user_id", "product_id", "event_type")): DataFrame = {

    val dfWithKey = df.withColumn(
      "dedup_key",
      concat_ws("|", keyColumns.map(col): _*)
    )

    val uniqueKeys = dfWithKey
      .select("dedup_key")
      .distinct()
      .collect()
      .map(_.getAs[String]("dedup_key"))
      .filterNot(isSeen)

    markSeenBatch(uniqueKeys)

    val uniqueKeysSet = uniqueKeys.toSet
    dfWithKey.filter(col("dedup_key").isin(uniqueKeysSet.toSeq: _*))
      .drop("dedup_key")
  }

  def persistToDelta(
                      spark: SparkSession,
                      bucketName: String,
                      dedupPath: String
                    ): Try[Unit] = {
    try {
      if (seenKeys.isEmpty) {
        logger.debug("No deduplication keys to persist")
        return Success(())
      }

      import spark.implicits._
      val keysDF = seenKeys.toSeq.map { key =>
        (key, System.currentTimeMillis())
      }.toDF("dedup_key", "dedup_timestamp")

      val fullPath = s"s3a://$bucketName/$dedupPath"
      val tableExists = scala.util.Try {
        DeltaTable.forPath(spark, fullPath)
      }.isSuccess

      if (tableExists) {
        val deltaTable = DeltaTable.forPath(spark, fullPath)
        deltaTable
          .as("target")
          .merge(
            keysDF.as("source"),
            "target.dedup_key = source.dedup_key"
          )
          .whenNotMatched()
          .insertAll()
          .execute()
      } else {
        MinioUtils.writeDeltaTable(
          df = keysDF,
          bucketName = bucketName,
          path = dedupPath,
          saveMode = SaveMode.Overwrite
        )
      }

      logger.info(s"Persisted ${seenKeys.size} deduplication keys to $dedupPath")
      Success(())
    } catch {
      case ex: Exception =>
        logger.error("Failed to persist deduplication keys", ex)
        Failure(ex)
    }
  }

  def getCacheSize: Int = seenKeys.size

  def clearCache(): Unit = {
    val size = seenKeys.size
    seenKeys.clear()
    logger.info(s"Cleared deduplication cache ($size keys removed)")
  }
}
