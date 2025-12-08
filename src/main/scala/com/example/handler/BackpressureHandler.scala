package com.example.handler

import org.apache.spark.sql.streaming.StreamingQuery
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}


object BackpressureHandler {
  private val logger = LoggerFactory.getLogger(getClass)

  case class BackpressureConfig(
                                 maxLagRecords: Long = 100000,
                                 maxLagSeconds: Long = 300,
                                 pauseOnBackpressure: Boolean = false,
                                 checkIntervalSeconds: Int = 60
                               )

  def checkBackpressure(
                         query: StreamingQuery,
                         config: BackpressureConfig = BackpressureConfig()
                       ): Try[BackpressureResult] = {
    try {
      if (!query.isActive) {
        return Success(BackpressureResult(
          hasBackpressure = false,
          message = "Query is not active"))
      }

      val progress = query.lastProgress

      if (progress == null) {
        return Success(BackpressureResult(
          hasBackpressure = false,
          message = "No progress information available yet"))
      }

      val sources = progress.sources
      var maxInputRowsPerSecond = 0.0
      var maxProcessedRowsPerSecond = 0.0
      var totalInputRows = 0L
      var totalProcessedRows = 0L

      sources.foreach { source =>
        val inputRowsPerSecond = Option(source.inputRowsPerSecond).getOrElse(0.0)
        val processedRowsPerSecond = Option(source.processedRowsPerSecond).getOrElse(0.0)
        val inputRows = Option(source.numInputRows).getOrElse(0L)
        val processedRows = 0L

        maxInputRowsPerSecond = math.max(maxInputRowsPerSecond, inputRowsPerSecond)
        maxProcessedRowsPerSecond = math.max(maxProcessedRowsPerSecond, processedRowsPerSecond)
        totalInputRows += inputRows
        totalProcessedRows += processedRows
      }

      val lagRecords = totalInputRows - totalProcessedRows
      val lagRatio = if (maxInputRowsPerSecond > 0) {
        (maxInputRowsPerSecond - maxProcessedRowsPerSecond) / maxInputRowsPerSecond
      } else {
        0.0
      }

      val hasBackpressure = lagRecords > config.maxLagRecords || lagRatio > 0.5

      val message = if (hasBackpressure) {
        f"Backpressure detected: lag=$lagRecords records, inputRate=$maxInputRowsPerSecond%.2f/s, " +
          f"processedRate=$maxProcessedRowsPerSecond%.2f/s, lagRatio=${lagRatio * 100}%.2f%%"
      } else {
        f"No backpressure: lag=$lagRecords records, inputRate=$maxInputRowsPerSecond%.2f/s, " +
          f"processedRate=$maxProcessedRowsPerSecond%.2f/s"
      }


      Success(BackpressureResult(
        hasBackpressure = hasBackpressure,
        message = message,
        shouldPause = hasBackpressure && config.pauseOnBackpressure,
        lagRecords = lagRecords,
        lagRatio = lagRatio,
        inputRate = maxInputRowsPerSecond,
        processedRate = maxProcessedRowsPerSecond
      ))
    } catch {
      case ex: Exception =>
        logger.error("Error checking backpressure", ex)
        Failure(ex)
    }
  }

  case class BackpressureResult(
                                 hasBackpressure: Boolean,
                                 message: String,
                                 shouldPause: Boolean = false,
                                 lagRecords: Long = 0L,
                                 lagRatio: Double = 0.0,
                                 inputRate: Double = 0.0,
                                 processedRate: Double = 0.0
                               )

  def monitorBackpressure(
                           query: StreamingQuery,
                           config: BackpressureConfig = BackpressureConfig(),
                           onBackpressure: BackpressureResult => Unit
                         ): Thread = {
    val monitorThread = new Thread(new Runnable {
      override def run(): Unit = {
        logger.info("Backpressure monitor started")
        while (query.isActive) {
          try {
            checkBackpressure(query, config) match {
              case Success(result) =>
                if (result.hasBackpressure) {
                  onBackpressure(result)
                } else {
                  logger.debug(result.message)
                }
              case Failure(ex) =>
                logger.error("Error checking backpressure", ex)
            }
            Thread.sleep(config.checkIntervalSeconds * 1000)
          } catch {
            case _: InterruptedException =>
              logger.info("Backpressure monitor interrupted")
              Thread.currentThread().interrupt()
              return
            case ex: Exception =>
              logger.error("Error in backpressure monitor", ex)
          }
        }
        logger.info("Backpressure monitor stopped")
      }
    }, "BackpressureMonitor")

    monitorThread.setDaemon(true)
    monitorThread.start()
    monitorThread
  }

  def getDefaultConfig: BackpressureConfig = {
    val maxLagRecords = sys.env.get("BACKPRESSURE_MAX_LAG_RECORDS")
      .flatMap(v => scala.util.Try(v.toLong).toOption)
      .getOrElse(100000L)

    val maxLagSeconds = sys.env.get("BACKPRESSURE_MAX_LAG_SECONDS")
      .flatMap(v => scala.util.Try(v.toLong).toOption)
      .getOrElse(300L)

    val pauseOnBackpressure = sys.env.get("BACKPRESSURE_PAUSE_ON_BACKPRESSURE")
      .exists(_.toLowerCase == "true")

    BackpressureConfig(
      maxLagRecords = maxLagRecords,
      maxLagSeconds = maxLagSeconds,
      pauseOnBackpressure = pauseOnBackpressure)
  }
}
