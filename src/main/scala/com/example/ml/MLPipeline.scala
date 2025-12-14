package com.example.ml

import com.example.main.BATCH
import com.example.tool.CSVToKafka
import org.slf4j.LoggerFactory

object MLPipeline {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    logger.info("=" * 40)
    logger.info("Starting ML Pipeline...")
    logger.info("=" * 40)

    CSVToKafka.run("data/2019-Oct-1.csv")
    BATCH.main(args)
    ALSTraining.train()
    ClassificationTraining.train()

    logger.info("---------ML Pipeline Completed---------")
  }
}
