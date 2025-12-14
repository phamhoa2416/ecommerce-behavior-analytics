package com.example.ml

import com.example.config.AppConfig
import com.example.handler.RetryHandler
import com.example.util.{MLUtils, MinioUtils, SparkUtils}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object ClassificationTraining {
  private val logger = LoggerFactory.getLogger(getClass)

  def train(): Unit = {
    logger.info("Starting Spark ML Classification model training")
    val spark = SparkUtils.createSparkSession("ClassificationModel")
    import spark.implicits._

    try {
      val data: DataFrame = MLUtils.loadDataFromMinio(spark, logger)

      val maxDate = data.select(
        max(unix_timestamp($"event_time"))
      ).head().getLong(0)
      val decayRate = AppConfig.SPARK_ML_TIME_DECAY_FACTOR

      val sessionWindow = Window.partitionBy("user_session").orderBy("event_time")

      val processedData = data
        .withColumn("hour", hour($"event_time"))
        .withColumn("week_day", dayofweek($"event_time"))
        .withColumn("prev_event_time", lag("event_time", 1).over(sessionWindow))
        .withColumn("secs_since_last",
          unix_timestamp($"event_time") - unix_timestamp($"prev_event_time")
        ).na.fill(0, Array("secs_since_last"))
        .withColumn("label",
          when($"event_type" === "purchase", 2.0)
            .when($"event_type" === "cart", 1.0)
            .otherwise(0.0)
        )
        .withColumn("event_weight",
          when($"event_type" === "cart", AppConfig.SPARK_ML_CLS_EVENT_WEIGHT_CART)
            .when($"event_type" === "purchase", AppConfig.SPARK_ML_CLS_EVENT_WEIGHT_PURCHASE)
            .when($"event_type" === "remove_from_cart", AppConfig.SPARK_ML_CLS_EVENT_WEIGHT_REMOVE_FROM_CART)
            .when($"event_type" === "view", AppConfig.SPARK_ML_CLS_EVENT_WEIGHT_VIEW)
            .otherwise(0.0)
        )
        .withColumn("time_diff", lit(maxDate) - unix_timestamp($"event_time"))
        .withColumn("time_diff_frac", $"time_diff" / 86400.0)
        .withColumn("total_weight", $"event_weight" * exp(lit(-decayRate) * $"time_diff_frac"))
        .withColumn("weight", when($"total_weight" < 0.01, 0.01).otherwise($"total_weight"))
        .cache()

      val totalRecords = processedData.count()
      logger.info(s"Processed data: $totalRecords records with temporal features and weights")

      val labelCounts = processedData.groupBy("label").count().collect()
      val labelDistribution = labelCounts.map(row => s"${row.getDouble(0)} -> ${row.getLong(1)}").mkString(", ")
      logger.info(s"Label distribution: [$labelDistribution]")

      val weightStats = processedData.select(
        min("weight").as("min_weight"),
        max("weight").as("max_weight"),
        avg("weight").as("avg_weight")
      ).head()
      logger.info(f"Weight statistics - Min: ${weightStats.getDouble(0)}%.4f, Max: ${weightStats.getDouble(1)}%.4f, Avg: ${weightStats.getDouble(2)}%.4f")

      // Choose between Top-N bucketing or brute-force (all categories)
      val useTopNBucketing = AppConfig.SPARK_ML_CLS_USE_TOP_N_BUCKETING

      val (trainingData, categoryCol, brandCol, maxBins) = if (useTopNBucketing) {
        // Top-N bucketing for high-cardinality categorical features
        val topN = AppConfig.SPARK_ML_CLS_TOP_N_CATEGORIES
        logger.info(s"Using Top-$topN bucketing strategy for categorical features")

        val topBrands = processedData.groupBy("brand")
          .count()
          .orderBy(desc("count"))
          .limit(topN)
          .select("brand")
          .as[String]
          .collect()
          .toSet

        val topCategories = processedData.groupBy("category_code")
          .count()
          .orderBy(desc("count"))
          .limit(topN)
          .select("category_code")
          .as[String]
          .collect()
          .toSet

        logger.info(s"Selected top $topN brands (${topBrands.size} unique) and categories (${topCategories.size} unique)")

        val bucketedData = processedData
          .withColumn("brand_bucketed",
            when($"brand".isin(topBrands.toSeq: _*), $"brand")
              .otherwise("other")
          )
          .withColumn("category_bucketed",
            when($"category_code".isin(topCategories.toSeq: _*), $"category_code")
              .otherwise("other")
          )
          .cache()

        logger.info(s"Bucketed data: ${bucketedData.count()} records")
        processedData.unpersist()
        (bucketedData, "category_bucketed", "brand_bucketed", AppConfig.SPARK_ML_CLS_MAX_BINS)
      } else {
        logger.info("Using brute-force mode: all categorical values preserved")

        val brandCardinality = processedData.select("brand").distinct().count().toInt
        val categoryCardinality = processedData.select("category_code").distinct().count().toInt
        val dynamicMaxBins = math.max(brandCardinality, categoryCardinality) + 10

        logger.info(s"Categorical cardinality - Brands: $brandCardinality, Categories: $categoryCardinality")
        logger.info(s"Dynamic maxBins set to: $dynamicMaxBins")

        (processedData, "category_code", "brand", dynamicMaxBins)
      }

      val categoryIndexer = new StringIndexer()
        .setInputCol(categoryCol)
        .setOutputCol("category_idx")
        .setHandleInvalid("keep")

      val brandIndexer = new StringIndexer()
        .setInputCol(brandCol)
        .setOutputCol("brand_idx")
        .setHandleInvalid("keep")

      val assembler = new VectorAssembler()
        .setInputCols(Array(
          "category_idx",
          "brand_idx",
          "price",
          "hour",
          "week_day",
          "secs_since_last"
        ))
        .setOutputCol("features")
        .setHandleInvalid("keep")

      val rf = new RandomForestClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setWeightCol("weight")
        .setPredictionCol("prediction")
        .setRawPredictionCol("raw_prediction")
        .setProbabilityCol("probability")
        .setNumTrees(AppConfig.SPARK_ML_CLS_TREES)
        .setMaxDepth(AppConfig.SPARK_ML_CLS_MAX_DEPTH)
        .setMinInstancesPerNode(AppConfig.SPARK_ML_CLS_MIN_INSTANCES_PER_NODE)
        .setMaxBins(maxBins)
        .setSubsamplingRate(AppConfig.SPARK_ML_CLS_SUBSAMPLING_RATE)
        .setFeatureSubsetStrategy(AppConfig.SPARK_ML_CLS_FEATURE_SUBSET_STRATEGY)
        .setSeed(36)

      val pipeline = new Pipeline()
        .setStages(Array(
          categoryIndexer,
          brandIndexer,
          assembler,
          rf
        ))

      val Array(training, test) = MLUtils.prepareModelData(trainingData, logger)

      logger.info("Training the Random Forest Classification model...")
      val startTime = System.currentTimeMillis()
      val model = pipeline.fit(training)
      val trainingTime = (System.currentTimeMillis() - startTime) / 1000.0
      logger.info(s"Model training completed in ${MLUtils.parseTime(trainingTime)}")

      logger.info("Generating predictions on test data...")
      val predictions = model.transform(test).cache()
      logger.info(s"Generated ${predictions.count()} predictions on test data")

      logger.info("Evaluating model performance...")
      val confusionMatrix = predictions
        .groupBy("label")
        .pivot("prediction", Seq(0.0, 1.0, 2.0))
        .count()
        .na.fill(0)
        .orderBy("label")

      logger.info("=" * 40)
      logger.info("----------Confusion Matrix----------")
      confusionMatrix.show(false)
      logger.info("=" * 40)

      val predictionsAndLabels = predictions
        .select($"prediction", $"label")
        .as[(Double, Double)]
        .rdd

      val metrics = new MulticlassMetrics(predictionsAndLabels)

      logger.info("=" * 40)
      logger.info("Model Evaluation Metrics:")
      logger.info(f"Overall Accuracy: ${metrics.accuracy * 100}%.2f%%")
      logger.info(f"Weighted Precision: ${metrics.weightedPrecision}%.4f")
      logger.info(f"Weighted Recall: ${metrics.weightedRecall}%.4f")
      logger.info(f"Weighted F1 Score: ${metrics.weightedFMeasure}%.4f")
      logger.info("=" * 40)
      logger.info("Class-wise Metrics:")
      logger.info("-" * 40)
      metrics.labels.sorted.foreach { label =>
        val className = label match {
          case 0.0 => "View/Remove from Cart"
          case 1.0 => "Add to Cart"
          case 2.0 => "Purchase"
          case _ => "Unknown"
        }

        logger.info(s"Class: $className (Label: $label)")
        logger.info(f"  Precision: ${metrics.precision(label)}%.4f")
        logger.info(f"  Recall:    ${metrics.recall(label)}%.4f")
        logger.info(f"  F1 Score:  ${metrics.fMeasure(label)}%.4f")
        logger.info("-" * 40)
      }

      logger.info("Evaluating purchase probability statistics...")
      val getPurchaseProb = udf { v: Vector => v(2) }
      val scoredData = predictions
        .withColumn("prob_purchase", getPurchaseProb($"probability"))

      val K = 1000
      val topK = scoredData
        .select("prob_purchase", "label")
        .orderBy(desc("prob_purchase"))
        .limit(K)

      val actualBuyers = topK.filter($"label" === 2.0).count()
      val precisionAtK = actualBuyers.toDouble / K
      logger.info(f"Precision at Top $K for Purchase class: $precisionAtK%.4f")

      logger.info("Calculating Log Loss...")
      val logLossEvaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setProbabilityCol("probability")
        .setMetricName("logLoss")

      val logLoss = logLossEvaluator.evaluate(predictions)
      logger.info(f"Log Loss of the model: $logLoss%.4f (lower is better)")

      val modelBasePath = AppConfig.SPARK_ML_CLS_BASE_PATH
      logger.info("Saving Classification model...")
      RetryHandler.withRetry(
        MinioUtils.saveMLModel(
          model,
          AppConfig.MINIO_BUCKET_NAME,
          s"$modelBasePath/model"
        ),
        name = "Save Classification Model to MinIO"
      ) match {
        case Success(_) => logger.info("Classification model saved successfully to MinIO.")
        case Failure(ex) =>
          logger.error("Failed to save Classification model to MinIO after retries", ex)
          throw ex
      }

      logger.info("Saving model metadata...")
      RetryHandler.withRetry(
        MLUtils.saveModelMetadata(
          spark,
          AppConfig.MINIO_BUCKET_NAME,
          s"$modelBasePath/metadata",
          logger,
          Map(
            "timestamp" -> ZonedDateTime.now(ZoneId.of("UTC"))
              .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z")),
            "model_type" -> "RandomForestClassifier",
            "num_trees" -> AppConfig.SPARK_ML_CLS_TREES.toString,
            "max_depth" -> AppConfig.SPARK_ML_CLS_MAX_DEPTH.toString,
            "max_bins" -> maxBins.toString,
            "subsampling_rate" -> AppConfig.SPARK_ML_CLS_SUBSAMPLING_RATE.toString,
            "feature_subset_strategy" -> AppConfig.SPARK_ML_CLS_FEATURE_SUBSET_STRATEGY,
            "accuracy" -> f"${metrics.accuracy}%.4f",
            "weighted_precision" -> f"${metrics.weightedPrecision}%.4f",
            "weighted_recall" -> f"${metrics.weightedRecall}%.4f",
            "weighted_f1" -> f"${metrics.weightedFMeasure}%.4f",
            "log_loss" -> f"$logLoss%.4f",
            "precision_at_k" -> f"$precisionAtK%.4f",
            "total_records" -> totalRecords.toString,
            "training_time_secs" -> f"$trainingTime%.2f"
          )
        ),
        name = "Save Model Metadata to MinIO"
      ) match {
        case Success(_) => logger.info("Model metadata saved successfully to MinIO.")
        case Failure(ex) =>
          logger.error("Failed to save model metadata to MinIO after retries", ex)
          throw ex
      }

      logger.info("Model training and saving completed successfully")
    } catch {
      case NonFatal(ex) =>
        logger.error("An error occurred during model training.", ex)
        throw ex
    } finally {
      spark.stop()
      logger.info("Spark session stopped. Application finished.")
    }
  }
}
