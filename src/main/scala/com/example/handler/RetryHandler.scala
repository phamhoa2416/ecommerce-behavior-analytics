package com.example.handler

import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.{Failure, Success, Try}

object RetryHandler {
  private val logger = LoggerFactory.getLogger(getClass)

  case class RetryConfig(
                          maxRetries: Int = 3,
                          initialDelay: Duration = 1.second,
                          maxDelay: Duration = 30.seconds,
                          backoffMultiplier: Double = 2.0
                        )

  def withRetry[T](
                    operation: => T,
                    config: RetryConfig = RetryConfig(),
                    name: String = "operation"
                  ): Try[T] = {
    @tailrec
    def attempt(
                 attempts: Int,
                 delay: Duration
               ): Try[T] = {
      logger.info(s"$name: Attempt $attempts/${config.maxRetries}")

      Try(operation) match {
        case Success(result) =>
          if (attempts > 1) {
            logger.info(s"$name: Succeeded on attempt $attempts")
          }
          Success(result)
        case Failure(exception) if attempts < config.maxRetries =>
          val nextDelay = (delay * config.backoffMultiplier).min(config.maxDelay)
          logger.warn(
            s"$name: Attempt $attempts failed. " +
              s"Retrying in ${nextDelay.toSeconds}s. Error: ${exception.getMessage}"
          )

          Thread.sleep(delay.toMillis)
          attempt(attempts + 1, nextDelay)
        case Failure(exception) =>
          logger.error(
            s"$name: Failed after ${config.maxRetries} attempts",
            exception
          )
          Failure(exception)
      }
    }

    attempt(1, config.initialDelay)
  }

  def withRetryOnCondition[T](
                               operation: => T,
                               shouldRetry: Throwable => Boolean,
                               config: RetryConfig = RetryConfig(),
                               name: String = "operation"
                             ): Try[T] = {
    @tailrec
    def attempt(
                 attempts: Int,
                 delay: Duration
               ): Try[T] = {
      Try(operation) match {
        case Success(result) =>
          Success(result)
        case Failure(exception) if shouldRetry(exception) && attempts < config.maxRetries =>
          val nextDelay = (delay * config.backoffMultiplier).min(config.maxDelay)
          logger.warn(
            s"$name: Retryable failure on attempt $attempts. " +
              s"Retrying in ${nextDelay.toSeconds}s"
          )
          Thread.sleep(delay.toMillis)
          attempt(attempts + 1, nextDelay)
        case Failure(exception) =>
          if (attempts >= config.maxRetries) {
            logger.error(s"$name: Max retries exceeded", exception)
          } else {
            logger.error(s"$name: Non-retryable failure", exception)
          }
          Failure(exception)
      }
    }

    attempt(1, config.initialDelay)
  }

  def isRetryableException(exception: Throwable): Boolean = {
    exception match {
      case _: java.net.SocketTimeoutException => true
      case _: java.net.ConnectException => true
      case _: java.io.IOException => true
      case e if e.getMessage != null =>
        val msg = e.getMessage.toLowerCase
        msg.contains("timeout") ||
          msg.contains("connection") ||
          msg.contains("network") ||
          msg.contains("temporary") ||
          msg.contains("unavailable")
      case _ => false
    }
  }
}
