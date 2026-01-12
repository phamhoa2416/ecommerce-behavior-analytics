package com.example.util

import com.clickhouse.jdbc.ClickHouseDataSource
import com.example.config.AppConfig
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource
import scala.util.{Failure, Success, Try}

/**
 * Utility object for managing ClickHouse database connections.
 * Provides connection pool initialization, connection retrieval, and transaction management
 * for efficient database operations.
 */
object ClickHouseUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  // Connection pool state
  private var dataSource: Option[DataSource] = None
  private var connectionProps: Option[Properties] = None

  /**
   * Initializes the ClickHouse connection pool with the specified configuration.
   *
   * This function creates a connection pool that can be reused across multiple database operations.
   * It validates the connection by attempting to establish a test connection before completing.
   *
   * @param url The ClickHouse JDBC connection URL
   * @param user The database username
   * @param password The database password
   * @param batchSize The batch size for bulk operations
   * @param maxConnections Maximum number of connections in the pool
   * @param connectionTimeout Connection timeout in milliseconds
   * @return Success(()) if initialization succeeds, Failure with exception otherwise
   */
  def initialize(
                  url: String,
                  user: String,
                  password: String,
                  batchSize: Int,
                  maxConnections: Int = AppConfig.clickhouseSettings.maxConnections,
                  connectionTimeout: Int = AppConfig.clickhouseSettings.connectionTimeoutMs
                ): Try[Unit] = {
    try {
      // Configure connection properties
      val props = new Properties()
      props.setProperty("user", user)
      props.setProperty("password", password)
      props.setProperty("batchsize", batchSize.toString)
      props.setProperty("socket_timeout", AppConfig.clickhouseSettings.socketTimeoutMs.toString)
      props.setProperty("connect_timeout", connectionTimeout.toString)

      // Create and store the data source
      val source = new ClickHouseDataSource(url)
      dataSource = Some(source)
      connectionProps = Some(props)

      // Validate connection by attempting to establish a test connection
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

  /**
   * Retrieves a connection from the connection pool.
   *
   * The connection is configured with auto-commit disabled to support transaction management.
   * Callers are responsible for closing the connection after use.
   *
   * @return Success(Connection) if a connection can be obtained, 
   *         Failure(IllegalStateException) if the pool is not initialized
   */
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

  /**
   * Executes a function within a database transaction, automatically handling commit/rollback.
   *
   * This function provides a safe way to execute database operations with automatic
   * transaction management. If the function completes successfully, the transaction
   * is committed. If an exception occurs, the transaction is rolled back.
   *
   * @param f The function to execute with the database connection
   * @tparam T The return type of the function
   * @return Success(result) if execution succeeds, Failure(exception) if an error occurs
   */
  def withConnection[T](f: Connection => T): Try[T] = {
    getConnection match {
      case Success(conn) =>
        try {
          val result = f(conn)
          // Commit transaction on successful execution
          conn.commit()
          Success(result)
        } catch {
          case ex: Exception =>
            // Rollback transaction on error
            conn.rollback()
            logger.error("Error executing with connection", ex)
            Failure(ex)
        } finally {
          // Always close the connection
          conn.close()
        }
      case Failure(ex) => Failure(ex)
    }
  }

  /**
   * Retrieves the connection properties used for the connection pool.
   *
   * @return Properties object containing connection configuration,
   *         or empty properties if pool is not initialized
   */
  def getConnectionProperties: Properties = {
    connectionProps.getOrElse {
      val props = new Properties()
      props.put("user", "")
      props.put("password", "")
      props
    }
  }

  /**
   * Closes the connection pool and releases all resources.
   *
   * This should be called during application shutdown to properly clean up resources.
   */
  def close(): Unit = {
    dataSource = None
    connectionProps = None
    logger.info("ClickHouse connection pool closed")
  }
}
