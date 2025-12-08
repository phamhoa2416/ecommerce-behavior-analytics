package com.example.validation

import com.example.util.MinioUtils
import com.example.validation.model.{DQMetrics, Result}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

object Validator {
  private val logger = LoggerFactory.getLogger(getClass)

  private val MIN_PRICE = 0.01
  private val MAX_PRICE = 1000000.0
  private val VALID_EVENT_TYPES = Set("view", "cart", "purchase", "remove_from_cart")

  def validateAndClean(df: DataFrame): Result = {
    import df.sparkSession.implicits._

    logger.info(s"Total records to validate: ${df.count()}")

    val validated = df
      .withColumn("is_valid_price", col("price").isNotNull && col("price") >= MIN_PRICE && col("price") <= MAX_PRICE)
      .withColumn("is_valid_event_type", col("event_type").isNotNull && col("event_type").isin(VALID_EVENT_TYPES.toSeq: _*))
      .withColumn("is_valid_product_id", col("product_id").isNotNull && col("product_id") > 0)
      .withColumn("is_valid_user_id", col("user_id").isNotNull && col("user_id") > 0)
      .withColumn("is_valid_event_time", col("event_time").isNotNull)
      .withColumn("is_valid_category_id", col("category_id").isNotNull && col("category_id") > 0)
      .withColumn(
        "is_valid_record",
        col("is_valid_price") &&
          col("is_valid_event_type") &&
          col("is_valid_product_id") &&
          col("is_valid_user_id") &&
          col("is_valid_event_time") &&
          col("is_valid_category_id")
      )
      .withColumn(
        "invalid_reasons",
        concat_ws(
          ", ",
          when(!col("is_valid_price"), lit("invalid_price")).otherwise(lit("")),
          when(!col("is_valid_event_type"), lit("invalid_event_type")).otherwise(lit("")),
          when(!col("is_valid_product_id"), lit("invalid_product_id")).otherwise(lit("")),
          when(!col("is_valid_user_id"), lit("invalid_user_id")).otherwise(lit("")),
          when(!col("is_valid_event_time"), lit("invalid_event_time")).otherwise(lit("")),
          when(!col("is_valid_category_id"), lit("invalid_category_id")).otherwise(lit(""))
        )
      )
      .cache()

    val counts = validated.agg(
      count(lit(1)).as("total"),
      sum(when(col("is_valid_record"), 1).otherwise(0)).as("valid"),
      sum(when(!col("is_valid_record"), 1).otherwise(0)).as("invalid")
    ).collect().head

    val totalCount = counts.getLong(0)
    val validCount = counts.getLong(1)
    val invalidCount = counts.getLong(2)

    val validRecords = validated
      .filter(col("is_valid_record"))
      .select(
        col("event_time"),
        col("event_type"),
        col("product_id"),
        col("category_id"),
        when(col("category_code").isNull || col("category_code") === "", lit("unknown"))
          .otherwise(col("category_code")).as("category_code"),
        when(col("brand").isNull || col("brand") === "", lit("unknown"))
          .otherwise(col("brand")).as("brand"),
        col("price"),
        col("user_id"),
        col("user_session")
      )

    val invalidRecords = validated
      .filter(!col("is_valid_record"))
      .withColumn("processing_timestamp", current_timestamp())

    val invalidReasonCounts =
      if (invalidCount > 0) {
        invalidRecords
          .select(explode(split(col("invalid_reasons"), ", ")).as("reason"))
          .filter(col("reason") =!= "")
          .groupBy("reason")
          .count()
          .collect()
          .map(row => row.getString(0) -> row.getLong(1))
          .toMap
      } else {
        Map.empty[String, Long]
      }

    val metrics = DQMetrics(
      totalRecords = totalCount,
      validRecords = validCount,
      invalidRecords = invalidCount,
      invalidReasons = invalidReasonCounts
    )

    logger.info(s"Validation complete: Valid=$validCount, Invalid=$invalidCount")
    invalidReasonCounts.foreach { case (reason, count) =>
      logger.warn(s"  - $reason: $count records")
    }

    validated.unpersist()
    Result(validRecords, invalidRecords, metrics)
  }

  def saveInvalidRecords(
                          invalidRecords: DataFrame,
                          bucketName: String,
                          path: String,
                          format: String = "delta",
                          autoOptimize: Boolean = false
                        ): Unit = {
    val invalidCount = invalidRecords.count()
    if (invalidCount == 0) {
      logger.info("No invalid records to save.")
      return
    }

    try {
      MinioUtils.writeDeltaTable(
        df = invalidRecords,
        bucketName = bucketName,
        path = path,
        saveMode = SaveMode.Append,
        partitionColumns = Some(Seq("processing_timestamp")),
        autoOptimize = autoOptimize
      )
      logger.info(s"Saved invalid records to $path (bucket=$bucketName, format=$format)")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to save invalid records to $path in bucket $bucketName", e)
    }
  }
}
