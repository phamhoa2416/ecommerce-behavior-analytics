package com.example.util

import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
 * Utility object for ALS (Alternating Least Squares) recommendation system operations.
 * Provides functions for generating, saving, and retrieving user recommendations,
 * as well as computing popular items for cold start scenarios.
 */
object ALSUtils {
	private val logger = LoggerFactory.getLogger(getClass)

	/**
	 * Generates top-K recommendations for all users using an ALS model and saves them to MinIO.
	 * 
	 * This function generates personalized recommendations for all users in the model,
	 * optionally maps integer IDs back to original string IDs using indexers, and persists
	 * the results as a Delta table in MinIO storage.
	 * 
	 * @param alsModel The trained ALS model used for generating recommendations
	 * @param bucketName The MinIO bucket name where recommendations will be stored
	 * @param path The path within the bucket for storing recommendations
	 * @param topK The number of top recommendations to generate per user (default: 10)
	 * @param userIndexer Optional StringIndexerModel to map user integer IDs back to original IDs
	 * @param itemIndexer Optional StringIndexerModel to map item integer IDs back to original IDs
	 * @throws Exception if writing to MinIO fails
	 */
	def generateAndSaveUserRecommendations(
																					alsModel: ALSModel,
																					bucketName: String,
																					path: String,
																					topK: Int = 10,
																					userIndexer: Option[StringIndexerModel] = None,
																					itemIndexer: Option[StringIndexerModel] = None
																				): Unit = {

		logger.info(s"Generating top-$topK recommendations for all users...")

		// Generate recommendations for all users using the ALS model
		val userRecs = alsModel.recommendForAllUsers(topK)

		// Flatten the recommendations array structure into individual rows
		// Each user's recommendations are stored as an array, which we explode into separate rows
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

		// Map integer IDs back to original string IDs if indexers are provided
		// This is necessary because ALS models work with integer indices internally
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


	/**
	 * Retrieves precomputed recommendations for a specific user from MinIO.
	 * 
	 * @param spark The SparkSession instance
	 * @param bucketName The MinIO bucket name containing recommendations
	 * @param path The path within the bucket where recommendations are stored
	 * @param userId The user ID to retrieve recommendations for
	 * @return DataFrame containing recommendations for the specified user, ordered by rank
	 */
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

	/**
	 * Computes and saves the most popular items based on user interaction data.
	 * 
	 * This function calculates item popularity scores using weighted event types,
	 * where different user actions (purchase, cart, view, etc.) contribute differently
	 * to the popularity score. The results are used for cold start recommendations
	 * when personalized recommendations are not available.
	 * 
	 * @param interactionData DataFrame containing user interaction events with columns:
	 *                       user_id, product_id, event_type
	 * @param bucketName The MinIO bucket name where popular items will be stored
	 * @param path The path within the bucket for storing popular items
	 * @param topK The number of top popular items to compute (default: 100)
	 * @param eventWeights Map of event types to their weight contributions:
	 *                    - purchase: positive weight (default: 3.0)
	 *                    - cart: positive weight (default: 10.0)
	 *                    - remove_from_cart: negative weight (default: -10.0)
	 *                    - view: positive weight (default: 1.0)
	 * @throws Exception if writing to MinIO fails
	 */
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

		// Build a conditional expression that assigns weights based on event type
		// This creates a CASE WHEN expression chain for efficient Spark evaluation
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


	/**
	 * Computes and saves the most popular items grouped by product category.
	 * 
	 * This function calculates category-specific popularity rankings, which are useful
	 * for providing recommendations when users browse specific product categories.
	 * The results are partitioned by category for efficient querying.
	 * 
	 * @param interactionData DataFrame containing user interaction events with columns:
	 *                       user_id, product_id, category_code, event_type
	 * @param bucketName The MinIO bucket name where category-based popular items will be stored
	 * @param path The path within the bucket for storing category-based popular items
	 * @param topKPerCategory The number of top items to compute per category (default: 20)
	 * @throws Exception if writing to MinIO fails
	 */
	def computeAndSavePopularItemsByCategory(
																						interactionData: DataFrame,
																						bucketName: String,
																						path: String,
																						topKPerCategory: Int = 20
																					): Unit = {

		logger.info(s"Computing top-$topKPerCategory popular items per category...")

		// Define window function to rank items within each category
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
			// Calculate popularity score: purchases weighted 3x + total interactions
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

	/**
	 * Retrieves the most popular items from MinIO storage.
	 * 
	 * @param spark The SparkSession instance
	 * @param bucketName The MinIO bucket name containing popular items
	 * @param path The path within the bucket where popular items are stored
	 * @param limit The maximum number of items to retrieve (default: 10)
	 * @return DataFrame containing the top popular items, ordered by rank
	 */
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

	/**
	 * Retrieves popular items for a specific product category from MinIO storage.
	 * 
	 * @param spark The SparkSession instance
	 * @param bucketName The MinIO bucket name containing category-based popular items
	 * @param path The path within the bucket where category-based popular items are stored
	 * @param categoryCode The category code to filter items by
	 * @param limit The maximum number of items to retrieve (default: 10)
	 * @return DataFrame containing popular items for the specified category, ordered by category rank
	 */
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

	/**
	 * Retrieves recommendations for a user with intelligent fallback strategy.
	 * 
	 * This function implements a three-tier recommendation strategy:
	 * 1. Personalized: Returns precomputed ALS recommendations if available
	 * 2. Hybrid: Combines available personalized recommendations with popular items
	 * 3. Cold Start: Returns only popular items when no personalized recommendations exist
	 * 
	 * @param spark The SparkSession instance
	 * @param userId The user ID to get recommendations for
	 * @param precomputedPath The path to precomputed personalized recommendations in MinIO
	 * @param popularItemsPath The path to popular items in MinIO
	 * @param bucketName The MinIO bucket name containing both recommendation datasets
	 * @param limit The maximum number of recommendations to return (default: 10)
	 * @return A tuple containing:
	 *         - DataFrame with recommendations (columns: user_id, product_id, score, rank)
	 *         - String indicating recommendation type: "personalized", "hybrid", or "cold_start"
	 */
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
			// Cold start scenario: user has no personalized recommendations
			// Return only popular items as fallback
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
}
