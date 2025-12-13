package com.example.util

import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

//noinspection ScalaUnusedSymbol
object ALSUtils {
	private val logger = LoggerFactory.getLogger(getClass)

	def generateAndSaveUserRecommendations(
																					alsModel: ALSModel,
																					bucketName: String,
																					path: String,
																					topK: Int = 10,
																					userIndexer: Option[StringIndexerModel] = None,
																					itemIndexer: Option[StringIndexerModel] = None
																				): Unit = {

		logger.info(s"Generating top-$topK recommendations for all users...")

		// Generate recommendations
		val userRecs = alsModel.recommendForAllUsers(topK)

		// Flatten the recommendations array
		val flatRecs = userRecs
			.select(
				col("userId"),
				explode(col("recommendations")).alias("rec")
			)
			.select(
				col("userId"),
				col("rec.productId").alias("productId"),
				col("rec.rating").alias("score"),
				row_number().over(
					org.apache.spark.sql.expressions.Window
						.partitionBy("userId")
						.orderBy(desc("rec.rating"))
				).alias("rank")
			)

		// Map back to original IDs if indexers provided
		val finalRecs = (userIndexer, itemIndexer) match {
			case (Some(userIdx), Some(itemIdx)) =>
				val userLabels = userIdx.labelsArray(0)
				val itemLabels = itemIdx.labelsArray(0)

				flatRecs
					.withColumn("original_user_id",
						element_at(lit(userLabels), col("userId") + 1))
					.withColumn("original_product_id",
						element_at(lit(itemLabels), col("productId") + 1))
					.select(
						col("original_user_id").alias("user_id"),
						col("original_product_id").alias("product_id"),
						col("score"),
						col("rank")
					)

			case _ =>
				logger.warn("No indexers provided - recommendations will use integer IDs")
				flatRecs
		}

		// Save to MinIO
		logger.info(s"Saving recommendations to s3a://$bucketName/$path")
		MinioUtils.writeDeltaTable(
			finalRecs,
			bucketName,
			path,
			SaveMode.Overwrite,
			partitionColumns = None,
			repartitionColumns = Some(Seq("user_id"))
		)

		logger.info(s"Successfully saved ${finalRecs.count()} recommendation records")
	}


	private def getRecommendationsForUser(
																				 spark: SparkSession,
																				 bucketName: String,
																				 path: String,
																				 userId: String
																			 ): DataFrame = {
		MinioUtils.readDeltaTable(spark, bucketName, path)
			.filter(col("user_id") === userId)
			.orderBy("rank")
	}

	def computeAndSavePopularItems(
																	interactionData: DataFrame,
																	bucketName: String,
																	path: String,
																	topK: Int = 100,
																	eventWeights: Map[String, Double] = Map(
																		"purchase" -> 3.0,
																		"cart" -> 10.0,
																		"remove_from_cart" -> -10.0,
																		"view" -> 1.0
																	)
																): Unit = {

		logger.info(s"Computing top-$topK popular items for cold start...")

		// Build weight expression
		val weightExpr = eventWeights.foldLeft(lit(0.0)) {
			case (expr, (eventType, weight)) =>
				when(col("event_type") === eventType, weight).otherwise(expr)
		}

		val popularItems = interactionData
			.withColumn("event_weight", weightExpr)
			.groupBy("product_id")
			.agg(
				sum("event_weight").alias("popularity_score"),
				count("*").alias("total_interactions"),
				countDistinct("user_id").alias("unique_users"),
				sum(when(col("event_type") === "purchase", 1).otherwise(0)).alias("purchase_count")
			)
			.orderBy(desc("popularity_score"))
			.limit(topK)
			.withColumn("rank", row_number().over(
				Window.orderBy(desc("popularity_score"))
			))

		// Save to MinIO
		logger.info(s"Saving popular items to s3a://$bucketName/$path")
		MinioUtils.writeDeltaTable(
			popularItems,
			bucketName,
			path,
			SaveMode.Overwrite
		)

		logger.info(s"Successfully saved ${popularItems.count()} popular items")
	}


	def computeAndSavePopularItemsByCategory(
																						interactionData: DataFrame,
																						bucketName: String,
																						path: String,
																						topKPerCategory: Int = 20
																					): Unit = {

		logger.info(s"Computing top-$topKPerCategory popular items per category...")

		val categoryWindow = org.apache.spark.sql.expressions.Window
			.partitionBy("category_code")
			.orderBy(desc("popularity_score"))

		val popularByCategory = interactionData
			.filter(col("category_code").isNotNull && col("category_code") =!= "unknown")
			.groupBy("category_code", "product_id")
			.agg(
				count("*").alias("interaction_count"),
				countDistinct("user_id").alias("unique_users"),
				sum(when(col("event_type") === "purchase", 1).otherwise(0)).alias("purchase_count")
			)
			.withColumn("popularity_score",
				col("purchase_count") * 3 + col("interaction_count"))
			.withColumn("category_rank", row_number().over(categoryWindow))
			.filter(col("category_rank") <= topKPerCategory)

		// Save to MinIO
		logger.info(s"Saving category-based popular items to s3a://$bucketName/$path")
		MinioUtils.writeDeltaTable(
			popularByCategory,
			bucketName,
			path,
			SaveMode.Overwrite,
			partitionColumns = Some(Seq("category_code"))
		)

		logger.info(s"Successfully saved category-based popular items")
	}

	private def getPopularItems(
															 spark: SparkSession,
															 bucketName: String,
															 path: String,
															 limit: Int = 10
														 ): DataFrame = {
		MinioUtils.readDeltaTable(spark, bucketName, path)
			.orderBy("rank")
			.limit(limit)
	}

	def getPopularItemsByCategory(
																 spark: SparkSession,
																 bucketName: String,
																 path: String,
																 categoryCode: String,
																 limit: Int = 10
															 ): DataFrame = {
		MinioUtils.readDeltaTable(spark, bucketName, path)
			.filter(col("category_code") === categoryCode)
			.orderBy("category_rank")
			.limit(limit)
	}

	def getRecommendationsWithFallback(
																			spark: SparkSession,
																			userId: String,
																			precomputedPath: String,
																			popularItemsPath: String,
																			bucketName: String,
																			limit: Int = 10
																		): (DataFrame, String) = {

		val personalizedRecs = getRecommendationsForUser(spark, bucketName, precomputedPath, userId)
		val recCount = personalizedRecs.count()

		if (recCount >= limit) {
			logger.info(s"Returning personalized recommendations for user: $userId")
			(personalizedRecs.limit(limit), "personalized")
		} else if (recCount > 0) {
			logger.info(s"Hybrid recommendations for user: $userId (found $recCount personalized)")
			val popular = getPopularItems(spark, bucketName, popularItemsPath, limit - recCount.toInt)
				.select(
					lit(userId).alias("user_id"),
					col("product_id"),
					col("popularity_score").alias("score"),
					(col("rank") + lit(recCount)).alias("rank")
				)
			(personalizedRecs.union(popular).limit(limit), "hybrid")
		} else {
			// Cold start: return popular items
			logger.info(s"Cold start fallback for user: $userId")
			val popular = getPopularItems(spark, bucketName, popularItemsPath, limit)
				.select(
					lit(userId).alias("user_id"),
					col("product_id"),
					col("popularity_score").alias("score"),
					col("rank")
				)
			(popular, "cold_start")
		}
	}

	def saveModelMetadata(
												 spark: SparkSession,
												 bucketName: String,
												 path: String,
												 metadata: Map[String, String]
											 ): Unit = {
		import spark.implicits._

		val metadataWithTimestamp = metadata + (
			"saved_at" -> java.time.Instant.now().toString
			)

		val metadataDf = metadataWithTimestamp.toSeq.toDF("key", "value")

		logger.info(s"Saving model metadata to s3a://$bucketName/$path")
		MinioUtils.writeDeltaTable(
			metadataDf,
			bucketName,
			path,
			SaveMode.Overwrite
		)
	}

	def loadModelMetadata(
												 spark: SparkSession,
												 bucketName: String,
												 path: String
											 ): Map[String, String] = {
		import spark.implicits._

		MinioUtils.readDeltaTable(spark, bucketName, path)
			.as[(String, String)]
			.collect()
			.toMap
	}
}
