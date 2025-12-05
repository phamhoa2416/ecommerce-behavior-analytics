package com.example.validation.model

case class DQMetrics(
                  totalRecords: Long,
                  validRecords: Long,
                  invalidRecords: Long,
                  invalidReasons: Map[String, Long]
                  )
