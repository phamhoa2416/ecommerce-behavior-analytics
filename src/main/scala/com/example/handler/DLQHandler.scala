package com.example.handler

import com.example.AppConfig
import com.example.util.MinioUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.util.{Failure, Success, Try}

object DLQHandler {
  private val logger = LoggerFactory.getLogger(getClass)

  private case class DLQRecord(
                              records: DataFrame,
                              path: String,
                              bucketName: String,
                              batchId: Long,
                              reason: String,
                              retryCount: Int
                              )

  private var dlqQueue: Option[BlockingQueue[DLQRecord]] = None
  private var processorThread: Option[Thread] = None
  private var isRunning = false

  def start(spark: SparkSession): Unit = {
    if (isRunning) {
      logger.warn("Dead Letter Queue Handler is already running")
      return
    }

    dlqQueue = Some(new LinkedBlockingQueue[DLQRecord](AppConfig.applicationConfig.dlq.queueSize))
    isRunning = true
    val thread = new Thread(new DLQProcessor(spark), "Async Dead Letter Queue Processor")
    thread.setDaemon(true)
    thread.start()
    processorThread = Some(thread)
    logger.info("Async Dead Letter Queue started")
  }

  def stop(): Unit = {
    isRunning = false
    processorThread.foreach(_.interrupt())
    logger.info("Async Dead Letter Queue Handler stopped")
  }

  def writeToDLQ(
                records: DataFrame,
                path: String,
                bucketName: String,
                batchId: Long,
                reason: String,
                retryCount: Int
                ): Try[Unit] = {
    try {
      val queue = dlqQueue.getOrElse {
        return Failure(new IllegalStateException("DLQ handler not started. Call start() first."))
      }
      
      val dlqRecord = DLQRecord(records, path, bucketName, batchId, reason, retryCount)

      val offerTimeout = AppConfig.applicationConfig.dlq.offerTimeoutSeconds
      if (!queue.offer(dlqRecord, offerTimeout, TimeUnit.SECONDS)) {
        logger.error(s"DLQ queue is full! Failed to enqueue records for batch_id = $batchId")
        return Failure(new RuntimeException("DLQ queue is full"))
      }

      logger.info(s"Enqueued ${records.count()} records to DLQ queue (batch_id = $batchId, reason = $reason)")
      Success(())
    } catch {
      case ex: InterruptedException =>
        logger.error("Interrupted while enqueueing to DLQ", ex)
        Failure(ex)
      case ex: Exception =>
        logger.error(s"Failed to enqueue records to DLQ (batch_id=$batchId)", ex)
        Failure(ex)
    }
  }

  def getQueueSize: Int = dlqQueue.map(_.size()).getOrElse(0)

  private class DLQProcessor(session: SparkSession) extends Runnable {
    override def run(): Unit = {
      logger.info("DLQ Processor thread started")

      val queue = dlqQueue.getOrElse {
        logger.error("DLQ queue not initialized")
        return
      }
      
      while (isRunning || !queue.isEmpty) {
        try {
          val pollTimeout = com.example.AppConfig.applicationConfig.dlq.pollTimeoutSeconds
          val dlqRecord = queue.poll(pollTimeout, TimeUnit.SECONDS)

          if (dlqRecord != null) {
            processDLQRecord(dlqRecord)
          }
        } catch {
          case _: InterruptedException =>
            logger.info("DLQ Processor thread interrupted")
            Thread.currentThread().interrupt()
            return
          case ex: Exception =>
            logger.error("Error in DLQ processor thread", ex)
        }
      }

      logger.info("DLQ processor thread stopped")
    }

    private def processDLQRecord(record: DLQRecord): Unit = {
      try {

        val dlqRecords = record.records
          .select(
            col("event_time"),
            col("event_type"),
            col("product_id"),
            col("category_id"),
            col("category_code"),
            col("brand"),
            col("price"),
            col("user_id"),
            col("user_session"),
            lit(record.reason).alias("failure_reason"),
            current_timestamp().alias("failure_timestamp"),
            lit(record.batchId).alias("batch_id"),
            lit(record.retryCount).alias("retry_count")
          )
          .withColumn("year", year(col("failure_timestamp")))
          .withColumn("month", month(col("failure_timestamp")))
          .withColumn("day", dayofmonth(col("failure_timestamp")))

        MinioUtils.writeDeltaTable(
          df = dlqRecords,
          bucketName = record.bucketName,
          path = record.path,
          saveMode = SaveMode.Append,
          partitionColumns = Some(Seq("year", "month", "day"))
        )

        val count = record.records.count()
        logger.info(s"Successfully wrote $count records to DLQ at ${record.path} (batch_id=${record.batchId}, reason=${record.reason})")
      } catch {
        case e: Exception =>
          logger.error(s"Failed to write to DLQ at ${record.path} (batch_id=${record.batchId}, reason=${record.reason})")
      }
    }
  }

}
