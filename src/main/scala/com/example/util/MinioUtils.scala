package com.example.util

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.SparkSession
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

  def checkBucketExists(endpoint: String, accessKey: String, secretKey: String, bucket: String): Try[Unit] = {
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
}
