### Chưa kiểm tra toàn bộ flow 
## Các bước khởi tạo :
- Thêm 2 file data vào trong ./data/
- Run producer theo đường dẫn + --date YYYY:MM:DD
- Producer sẽ tự động produce data theo ngày yêu cầu đến Kafka Broker.
- Lưu raw tại kafka broker với topic raw_data_topic
- Airflow trigger Spark vào 9:30 hàng ngày ( chưa hiệu chỉnh)
- HDFS join với 2 trường là event_time và event_type
