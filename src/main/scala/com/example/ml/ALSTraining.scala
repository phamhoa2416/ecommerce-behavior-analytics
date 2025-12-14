package com.example.ml

import com.example.AppConfig
import com.example.handler.RetryHandler
import com.example.util.{ALSUtils, MLUtils, MinioUtils, SparkUtils}
import org.apache.spark.ml.evaluation.RankingEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object ALSTraining {
	private val logger = LoggerFactory.getLogger(getClass)

	def train(): Unit = {
		logger.info("Starting Spark ML ALS E-commerce recommendation model training...")
		val spark = SparkUtils.createSparkSession("ALSRecommendationModel")
		import spark.implicits._
		val cores = spark.sparkContext.defaultParallelism

		try {
			val data = MLUtils.loadDataFromMinio(spark, logger)

			val ratedData = data
				.withColumn("view_ratings",
					when($"event_type" === "view", AppConfig.SPARK_ML_ALS_EVENT_WEIGHT_VIEW).otherwise(0.0)
				)
				.withColumn("action_ratings",
					when($"event_type" === "cart", AppConfig.SPARK_ML_ALS_EVENT_WEIGHT_CART)
						.when($"event_type" === "purchase", AppConfig.SPARK_ML_ALS_EVENT_WEIGHT_PURCHASE)
						.when($"event_type" === "remove_from_cart", AppConfig.SPARK_ML_ALS_EVENT_WEIGHT_REMOVE_FROM_CART)
						.otherwise(0.0)
				)

			val maxDate = ratedData.select(
				max(unix_timestamp($"event_time"))
			).head().getLong(0)
			val decayRate = AppConfig.SPARK_ML_TIME_DECAY_FACTOR

			val ratedDataTemporal = ratedData
				.withColumn("time_diff", lit(maxDate) - unix_timestamp($"event_time"))
				.withColumn("time_diff_frac", $"time_diff" / 86400.0)
				.withColumn("view_score", $"view_ratings" * exp(lit(-decayRate) * $"time_diff_frac"))
				.withColumn("action_score", $"action_ratings" * exp(lit(-decayRate) * $"time_diff_frac"))

			val viewCap = AppConfig.SPARK_ML_ALS_EVENT_WEIGHT_VIEW_CAP
			val aggData = ratedDataTemporal
				.groupBy("user_id", "product_id")
				.agg(
					sum("view_score").alias("agg_view_score"),
					sum("action_score").alias("agg_action_score")
				)
				.withColumn("norm_view_score",
					when($"agg_view_score" > viewCap, viewCap).otherwise($"agg_view_score")
				)
				.withColumn("norm_action_score",
					when($"agg_action_score" < 0.0, 0.0).otherwise($"agg_action_score")
				)
				.withColumn("total_score", $"norm_view_score" + $"norm_action_score")
				.withColumn("log_score", log1p($"total_score"))

			logger.info(s"Prepared data for ALS model with ${aggData.count()} user-product pairs.")

			val userIndexer = new StringIndexer()
				.setInputCol("user_id")
				.setOutputCol("user_idx")
				.setHandleInvalid("keep")

			val productIndexer = new StringIndexer()
				.setInputCol("product_id")
				.setOutputCol("product_idx")
				.setHandleInvalid("keep")

			val userIndexerModel = userIndexer.fit(aggData)
			val indexedDataWithUser = userIndexerModel.transform(aggData)

			val productIndexerModel = productIndexer.fit(indexedDataWithUser)
			val indexedData = productIndexerModel.transform(indexedDataWithUser)
				.select(
					$"user_idx".cast("int").alias("userId"),
					$"product_idx".cast("int").alias("productId"),
					$"log_score".cast("float").alias("rating")
				).cache()

			logger.info(s"Total indexed records: ${indexedData.count()}")
			val uniqueUsers = indexedData.select("userId").distinct().count()
			logger.info(s"Unique users: $uniqueUsers")
			logger.info(s"Unique products: ${indexedData.select("productId").distinct().count()}")

			val Array(training, test) = MLUtils.prepareModelData(indexedData, logger)

			val als = new ALS()
				.setNumBlocks(cores * AppConfig.SPARK_ML_ALS_BLOCK_FACTOR)
				.setUserCol("userId")
				.setItemCol("productId")
				.setRatingCol("rating")
				.setRank(AppConfig.SPARK_ML_ALS_RANK)
				.setMaxIter(AppConfig.SPARK_ML_ALS_MAX_ITERATION)
				.setRegParam(AppConfig.SPARK_ML_ALS_REGULARIZATION)
				.setImplicitPrefs(true)
				.setColdStartStrategy("drop")
				.setAlpha(AppConfig.SPARK_ML_ALS_IMPLICIT_ALPHA)
				.setSeed(36)

			logger.info("Start training ALS model...")
			val startTime = System.currentTimeMillis()
			val alsModel = als.fit(training)
			val trainingTime = (System.currentTimeMillis() - startTime) / 1000.0
			logger.info(s"ALS model training completed in ${MLUtils.parseTime(trainingTime)}")

			logger.info(s"Starting ALS model evaluation...")
			val K = AppConfig.SPARK_ML_ALS_EVALUATION_TOP_K

			logger.info(s"Generating top-$K recommendations for evaluation...")
			val userRecommendations = alsModel.recommendForAllUsers(K).cache()
			userRecommendations.count() // Materialize the recommendations

			logger.info(s"Generating ground truth for evaluation...")
			val groundTruth = test
				.groupBy("userId")
				.agg(collect_list("productId").alias("label_int"))

			logger.info(s"Preparing data for RankingEvaluator...")
			val evalData = userRecommendations
				.select(
					col("userId"),
					col("recommendations.productId").alias("prediction_int")
				)
				.join(groundTruth, Seq("userId"), "inner")
				.withColumn("prediction", expr("transform(prediction_int, x -> cast(x as double))"))
				.withColumn("label", expr("transform(label_int, x -> cast(x as double))"))
				.cache()

			logger.info(s"Total records for evaluation: ${evalData.count()}")

			val precisionEvaluator = new RankingEvaluator()
				.setMetricName("precisionAtK")
				.setK(K)
				.setPredictionCol("prediction")
				.setLabelCol("label")

			val recallEvaluator = new RankingEvaluator()
				.setMetricName("recallAtK")
				.setK(K)
				.setPredictionCol("prediction")
				.setLabelCol("label")

			val ndcgEvaluator = new RankingEvaluator()
				.setMetricName("ndcgAtK")
				.setK(K)
				.setPredictionCol("prediction")
				.setLabelCol("label")

			val mapEvaluator = new RankingEvaluator()
				.setMetricName("meanAveragePrecision")
				.setPredictionCol("prediction")
				.setLabelCol("label")

			// Calculate all metrics
			logger.info("Calculating precisionAtK...")
			val precisionAtK = precisionEvaluator.evaluate(evalData)
			logger.info("Calculating recallAtK...")
			val recallAtK = recallEvaluator.evaluate(evalData)
			logger.info("Calculating ndcgAtK...")
			val ndcgAtK = ndcgEvaluator.evaluate(evalData)
			logger.info("Calculating meanAveragePrecision...")
			val meanAveragePrecision = mapEvaluator.evaluate(evalData)

			logger.info("=" * 60)
			logger.info("RANKING METRICS EVALUATION RESULTS")
			logger.info("=" * 60)
			logger.info(f"Precision@$K:              $precisionAtK%.4f")
			logger.info(f"Recall@$K:                 $recallAtK%.4f")
			logger.info(f"NDCG@$K:                   $ndcgAtK%.4f")
			logger.info(f"Mean Average Precision:    $meanAveragePrecision%.4f")
			logger.info("=" * 60)

			logger.info("Calculating hit rate...")
			val hitRateData = evalData
				.withColumn("hits", size(array_intersect(col("prediction"), col("label"))))

			val totalUsers = hitRateData.count()
			val usersWithHits = hitRateData.filter(col("hits") > 0).count()
			val hitRate = usersWithHits.toDouble / totalUsers

			logger.info("Calculating item coverage...")
			val totalItems = indexedData.select("productId").distinct().count()
			val recommendedItems = userRecommendations
				.select(explode(col("recommendations.productId")).alias("productId"))
				.distinct()
				.count()
			val itemCoverage = recommendedItems.toDouble / totalItems

			logger.info("COVERAGE METRICS")
			logger.info("=" * 60)
			logger.info(f"Hit Rate@$K:               $hitRate%.4f ($usersWithHits/$totalUsers users)")
			logger.info(f"Item Coverage:             $itemCoverage%.4f ($recommendedItems/$totalItems items)")
			logger.info("=" * 60)

			val modelBasePath = AppConfig.SPARK_ML_ALS_BASE_PATH

			logger.info("Saving ALS model...")
			RetryHandler.withRetry(
				MinioUtils.saveMLModel(
					alsModel,
					AppConfig.MINIO_BUCKET_NAME,
					s"$modelBasePath/model"
				),
				name = "Save ALS Model to MinIO"
			) match {
				case Success(_) => logger.info("ALS model saved successfully to MinIO.")
				case Failure(ex) =>
					logger.error("Failed to save ALS model to MinIO after retries", ex)
					throw ex
			}

			logger.info("Saving user indexer model...")
			RetryHandler.withRetry(
				MinioUtils.saveIndexerModel(
					userIndexerModel,
					AppConfig.MINIO_BUCKET_NAME,
					s"$modelBasePath/user_indexer"
				),
				name = "Save User Indexer Model to MinIO"
			) match {
				case Success(_) => logger.info("User indexer model saved successfully to MinIO.")
				case Failure(ex) =>
					logger.error("Failed to save user indexer model to MinIO after retries", ex)
					throw ex
			}


			logger.info("Saving product indexer model...")
			RetryHandler.withRetry(
				MinioUtils.saveIndexerModel(
					productIndexerModel,
					AppConfig.MINIO_BUCKET_NAME,
					s"$modelBasePath/product_indexer"
				),
				name = "Save Product Indexer Model to MinIO"
			) match {
				case Success(_) => logger.info("Product indexer model saved successfully to MinIO.")
				case Failure(ex) =>
					logger.error("Failed to save product indexer model to MinIO after retries", ex)
					throw ex
			}

			RetryHandler.withRetry(
				MLUtils.saveModelMetadata(
					spark,
					AppConfig.MINIO_BUCKET_NAME,
					s"$modelBasePath/metadata",
					logger,
					Map(
						"timestamp" -> ZonedDateTime.now(ZoneId.of("UTC"))
							.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z")),
						"model_type" -> "ALS",
						"rank" -> AppConfig.SPARK_ML_ALS_RANK.toString,
						"max_iter" -> AppConfig.SPARK_ML_ALS_MAX_ITERATION.toString,
						"reg_param" -> AppConfig.SPARK_ML_ALS_REGULARIZATION.toString,
						"alpha" -> AppConfig.SPARK_ML_ALS_IMPLICIT_ALPHA.toString,
						"precision_at_k" -> f"$precisionAtK%.4f",
						"recall_at_k" -> f"$recallAtK%.4f",
						"ndcg_at_k" -> f"$ndcgAtK%.4f",
						"map" -> f"$meanAveragePrecision%.4f",
						"num_users" -> uniqueUsers.toString,
						"num_products" -> totalItems.toString
					),
				),
				name = "Save Model Metadata to MinIO"
			) match {
				case Success(_) => logger.info("Model metadata saved successfully to MinIO.")
				case Failure(ex) =>
					logger.error("Failed to save model metadata to MinIO after retries", ex)
					throw ex
			}

			logger.info("Generating and saving pre-computed recommendations...")
			RetryHandler.withRetry(
				ALSUtils.generateAndSaveUserRecommendations(
					alsModel,
					AppConfig.MINIO_BUCKET_NAME,
					s"$modelBasePath/recommendations/user_product",
					topK = K,
					userIndexer = Some(userIndexerModel),
					itemIndexer = Some(productIndexerModel)
				),
				name = "Generate and Save User Recommendations"
			) match {
				case Success(_) => logger.info("Pre-computed user recommendations saved successfully to MinIO.")
				case Failure(ex) =>
					logger.error("Failed to generate/save user recommendations to MinIO after retries", ex)
					throw ex
			}

			logger.info("Computing popular items for cold start...")
			RetryHandler.withRetry(
				ALSUtils.computeAndSavePopularItems(
					data,
					AppConfig.MINIO_BUCKET_NAME,
					s"$modelBasePath/cold_start/popular_items",
				),
				name = "Compute and Save Popular Items for Cold Start"
			) match {
				case Success(_) => logger.info("Popular items for cold start saved successfully to MinIO.")
				case Failure(ex) =>
					logger.error("Failed to compute/save popular items to MinIO after retries", ex)
					throw ex
			}

			logger.info("Computing category-based popular items...")
			RetryHandler.withRetry(
				ALSUtils.computeAndSavePopularItemsByCategory(
					data,
					AppConfig.MINIO_BUCKET_NAME,
					s"$modelBasePath/cold_start/popular_by_category",
				),
				name = "Compute and Save Popular Items by Category for Cold Start"
			) match {
				case Success(_) => logger.info("Popular items by category for cold start saved successfully to MinIO.")
				case Failure(ex) =>
					logger.error("Failed to compute/save popular items by category to MinIO after retries", ex)
					throw ex
			}

			logger.info("Model training and saving completed successfully")
		} catch {
			case NonFatal(ex) =>
				logger.error("An error occurred during model training", ex)
				throw ex
		} finally {
			spark.stop()
			logger.info("Spark session stopped. Application finished")
		}
	}
}
