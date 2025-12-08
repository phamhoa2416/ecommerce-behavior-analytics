package com.example.util

import com.clickhouse.jdbc.ClickHouseDataSource
import com.example.AppConfig
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource
import scala.util.{Failure, Success, Try}

object ClickHouseUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  private var dataSource: Option[DataSource] = None
  private var connectionProps: Option[Properties] = None

  def initialize(
                url: String,
                user: String,
                password: String,
                batchSize: Int,
                maxConnections: Int = AppConfig.clickhouseSettings.maxConnections,
                connectionTimeout: Int = AppConfig.clickhouseSettings.connectionTimeoutMs
                ): Try[Unit] = {
    try {
      val props = new Properties()
      props.put("user", user)
      props.put("password", password)
      props.put("batchsize", batchSize)
      props.put("socket_timeout", AppConfig.clickhouseSettings.socketTimeoutMs.toString)
      props.put("connect_timeout", connectionTimeout.toString)

      val source = new ClickHouseDataSource(url, props)
      dataSource = Some(source)
      connectionProps = Some(props)

      val testConnection = source.getConnection
      testConnection.close()

      logger.info("ClickHouse connection pool initialized")
      Success(())
    } catch {
      case ex: Exception =>
        logger.error("Failed to initialize ClickHouse connection pool", ex)
        Failure(ex)
    }
  }

  def getConnection: Try[Connection] = {
    dataSource match {
      case Some(source) =>
        Try {
          val conn = source.getConnection
          conn.setAutoCommit(false)
          conn
        }
      case None =>
        Failure(new IllegalStateException("Connection pool not initialized."))
    }
  }

  def withConnection[T](f: Connection => T): Try[T] = {
    getConnection match {
      case Success(conn) =>
        try {
          val result = f(conn)
          conn.commit()
          Success(result)
        } catch {
          case ex: Exception =>
            conn.rollback()
            logger.error("Error executing with connection", ex)
            Failure(ex)
        } finally {
          conn.close()
        }
      case Failure(ex) => Failure(ex)
    }
  }

  def getConnectionProperties: Properties = {
    connectionProps.getOrElse {
      val props = new Properties()
      props.put("user", "")
      props.put("password", "")
      props
    }
  }

  def close(): Unit = {
    dataSource = None
    connectionProps = None
    logger.info("ClickHouse connection pool closed")
  }

  def isInitialized: Boolean = dataSource.isDefined
}
