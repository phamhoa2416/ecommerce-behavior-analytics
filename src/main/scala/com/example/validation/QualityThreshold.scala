package com.example.validation

import com.example.validation.model.DQMetrics
import org.slf4j.LoggerFactory

import scala.util.Try


object QualityThreshold {
  private val logger = LoggerFactory.getLogger(getClass)

  case class ThresholdConfig(
                            minValidRecordRate: Double = 0.95,
                            maxInvalidRecordRate: Double = 0.05,
                            alertOnThreshold: Boolean = true,
                            pauseOnThreshold: Boolean = false
                            )

  case class ThresholdResult(
                            passed: Boolean,
                            validRecordRate: Double,
                            threshold: Double,
                            message: String,
                            shouldPause: Boolean = false
                            )

  def checkThreshold(
                    metrics: DQMetrics,
                    config: ThresholdConfig = ThresholdConfig()
                    ): ThresholdResult = {
    if (metrics.totalRecords == 0) {
      val result = ThresholdResult(
        passed = false,
        validRecordRate = 0.0,
        threshold = config.minValidRecordRate,
        message = "No records processed",
        shouldPause = config.pauseOnThreshold
      )

      logResult(result, config)
      return result
    }

    val validRecordRate = metrics.validRecords.toDouble / metrics.totalRecords.toDouble
    val invalidRecordRate = metrics.invalidRecords.toDouble / metrics.totalRecords.toDouble

    val passed = validRecordRate >= config.minValidRecordRate &&
                  invalidRecordRate <= config.maxInvalidRecordRate

    val result = if (passed) {
      ThresholdResult(
        passed = true,
        validRecordRate = validRecordRate,
        threshold = config.minValidRecordRate,
        message = f"Data quality passed: ${validRecordRate * 100}%.2f%% valid records"
      )
    } else {
      val message =
        if (validRecordRate < config.maxInvalidRecordRate) {
          f"Data quality threshold BREACHED: ${validRecordRate * 100}%.2f%% valid records " +
            f"(threshold: ${config.minValidRecordRate * 100}%.2f%%)"
        } else {
          f"Data quality threshold BREACHED: ${invalidRecordRate * 100}%.2f%% invalid records " +
            f"(threshold: ${config.maxInvalidRecordRate * 100}%.2f%%)"
        }

      ThresholdResult(
        passed = false,
        validRecordRate = validRecordRate,
        threshold = config.minValidRecordRate,
        message = message,
        shouldPause = config.pauseOnThreshold
      )
    }

    logResult(result, config)
    result
  }

  private def logResult(result: ThresholdResult, config: ThresholdConfig): Unit = {
    if (result.passed) {
      logger.info(s"[Quality Threshold] ${result.message}")
    } else {
      if (config.alertOnThreshold) {
        logger.error(s"[Quality Threshold] ALERT: ${result.message}")
        logger.error(s"[Quality Threshold] Total records: ${result.validRecordRate}")
      } else {
        logger.warn(s"s[Quality Threshold] ${result.message}")
      }

      if (result.shouldPause) {
        logger.error(s"[Quality Threshold] Pipeline should be passed due to quality threshold breach")
      }
    }
  }


  def getDefaultConfig: ThresholdConfig = {
    val minValidRate = sys.env.get("MIN_VALID_RATE")
      .flatMap(v => Try(v.toDouble).toOption)
      .getOrElse(0.95)

    val maxInvalidRate = sys.env.get("MAX_INVALID_RATE")
      .flatMap(v => Try(v.toDouble).toOption)
      .getOrElse(0.05)

    val pauseOnThreshold = sys.env.get("PAUSE_ON_THRESHOLD")
      .exists(_.toLowerCase == "true")

    ThresholdConfig(
      minValidRecordRate = minValidRate,
      maxInvalidRecordRate = maxInvalidRate,
      pauseOnThreshold = pauseOnThreshold
    )
  }
}
