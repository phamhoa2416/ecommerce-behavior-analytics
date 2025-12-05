package com.example.validation.model

import org.apache.spark.sql.DataFrame

case class Result(
                 validRecords: DataFrame,
                 invalidRecords: DataFrame,
                 metrics: DQMetrics
                 )
