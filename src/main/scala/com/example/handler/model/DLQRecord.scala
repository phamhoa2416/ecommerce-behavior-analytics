package com.example.handler.model

import java.sql.Timestamp

case class DLQRecord(
                      event_time: Timestamp,
                      event_type: String,
                      product_id: Long,
                      category_id: Long,
                      category_code: String,
                      brand: String,
                      price: Double,
                      user_id: Long,
                      user_session: String,
                      failure_reason: String,
                      failure_timestamp: Timestamp,
                      batch_id: Long,
                      retry_count: Int
                    )
