package com.example.schema

import java.sql.Timestamp

final case class EcommerceEvent (
  event_time: Timestamp,
  event_type: String,
  product_id: Long,
  category_id: Long,
  category_code: String,
  brand: String,
  price: Double,
  user_id: Long,
  user_session: String
)


