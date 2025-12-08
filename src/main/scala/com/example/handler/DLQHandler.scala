package com.example.handler

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

  private val dlqQueue: BlockingQueue[DLQRecord] = new LinkedBlockingQueue[DLQRecord](10000)
  private var processorThread: Option[Thread] = None
  private var isRunning = false

  def start(spark: SparkSession): Unit = {
    if (isRunning) {
      logger.warn("Dead Letter Queue Handler is already running")
      return
    }

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
      val dlqRecord = DLQRecord(records, path, bucketName, batchId, reason, retryCount)

      if (!dlqQueue.offer(dlqRecord, 5, TimeUnit.SECONDS)) {
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

  def getQueueSize: Int = dlqQueue.size()

  private class DLQProcessor(session: SparkSession) extends Runnable {
    override def run(): Unit = {
      logger.info("DLQ Processor thread started")

      while (isRunning || !dlqQueue.isEmpty) {
        try {
          val dlqRecord = dlqQueue.poll(1, TimeUnit.SECONDS)

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
