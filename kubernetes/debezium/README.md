# Debezium CDC Setup với Strimzi Operator

Hướng dẫn triển khai Debezium Connect sử dụng **Strimzi Kafka Operator** để thực hiện Change Data Capture (CDC) từ PostgreSQL database sang Kafka.

## Tổng quan

Hệ thống này sử dụng:
- **Strimzi Kafka Operator**: Quản lý Kafka Connect cluster
- **KafkaConnect CRD**: Định nghĩa Kafka Connect cluster với Debezium image
- **KafkaConnector CRD**: Định nghĩa PostgreSQL connector configuration
- **PostgreSQL**: Database nguồn chứa dữ liệu ecommerce
- **Kafka**: Nhận dữ liệu CDC vào topic `ecommerce_batch_topic`

## Kiến trúc

```
PostgreSQL (ecommerce_db)
    ↓ [Logical Replication]
Strimzi KafkaConnect (Debezium)
    ↓ [CDC Events]
Kafka (ecommerce_batch_topic)
    ↓
Spark Batch Job
```

## Yêu cầu

- Kubernetes cluster đang chạy
- **Strimzi Kafka Operator** đã được cài đặt (trong namespace `kafka`)
- Kafka cluster `kafka-cluster` đã được triển khai (Strimzi)
- PostgreSQL database đã được setup với logical replication
- kubectl đã cấu hình

## Các bước triển khai

### 1. Tạo namespace

```bash
kubectl create namespace debezium
```

### 2. Tạo các Kafka topics cần thiết

Strimzi sẽ tự động tạo các topics cho Kafka Connect, nhưng bạn có thể tạo thủ công:

```bash
# Tạo topics cho Kafka Connect internal storage
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic debezium-connect-configs \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic debezium-connect-offsets \
  --partitions 25 \
  --replication-factor 1 \
  --if-not-exists

kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic debezium-connect-status \
  --partitions 5 \
  --replication-factor 1 \
  --if-not-exists

# Topic cho schema history
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic schema-changes.ecommerce_db \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

# Topic đích cho dữ liệu CDC (nếu chưa tồn tại)
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic ecommerce_batch_topic \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists
```

Hoặc sử dụng script:

```bash
chmod +x ./kubernetes/debezium/setup-kafka-topics.sh
./kubernetes/debezium/setup-kafka-topics.sh
```

### 3. Áp dụng Secret và RBAC

```bash
kubectl apply -f ./kubernetes/debezium/secret.yaml
kubectl apply -f ./kubernetes/debezium/rbac.yaml
```

### 4. Triển khai KafkaConnect cluster

```bash
kubectl apply -f ./kubernetes/debezium/kafkaconnect.yaml
```

Kiểm tra KafkaConnect đã sẵn sàng:

```bash
# Kiểm tra KafkaConnect resource
kubectl get kafkaconnect -n debezium

# Kiểm tra pods
kubectl get pods -n debezium -l strimzi.io/kind=KafkaConnect

# Kiểm tra logs
kubectl logs -f deployment/debezium-connect-cluster-connect -n debezium

# Kiểm tra service (tự động tạo bởi Strimzi)
kubectl get svc -n debezium
```

Đợi đến khi KafkaConnect cluster sẵn sàng:

```bash
kubectl wait --for=condition=Ready kafkaconnect/debezium-connect-cluster -n debezium --timeout=300s
```

### 5. Triển khai KafkaConnector

Sau khi KafkaConnect cluster đã sẵn sàng, triển khai connector:

```bash
kubectl apply -f ./kubernetes/debezium/kafkaconnector.yaml
```

Kiểm tra connector status:

```bash
# Kiểm tra KafkaConnector resource
kubectl get kafkaconnector -n debezium

# Xem chi tiết connector
kubectl describe kafkaconnector ecommerce-postgres-connector -n debezium

# Kiểm tra status
kubectl get kafkaconnector ecommerce-postgres-connector -n debezium -o jsonpath='{.status}'
```

### 6. Kiểm tra connector qua REST API

```bash
# Port-forward KafkaConnect service
kubectl port-forward svc/debezium-connect-cluster-connect-api -n debezium 8083:8083

# Trong terminal khác, kiểm tra connectors
curl http://localhost:8083/connectors

# Kiểm tra status của connector
curl http://localhost:8083/connectors/ecommerce-postgres-connector/status | jq .

# Kiểm tra config
curl http://localhost:8083/connectors/ecommerce-postgres-connector/config | jq .
```

Hoặc sử dụng script:

```bash
chmod +x ./kubernetes/debezium/check-connector.sh
./kubernetes/debezium/check-connector.sh
```

### 7. Test CDC

Thêm dữ liệu test vào PostgreSQL:

```bash
# Kết nối vào PostgreSQL (giả sử database đã được setup)
kubectl exec -it <postgres-pod> -n database -- \
  psql -U ecommerce_user -d ecommerce_db -c "
INSERT INTO ecommerce_events (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session)
VALUES 
  (NOW(), 'view', 12345, 678, 'electronics.smartphone', 'Apple', 999.99, 1001, 'session-001'),
  (NOW(), 'cart', 12346, 678, 'electronics.smartphone', 'Samsung', 899.99, 1002, 'session-002');
"

# Consume từ Kafka topic để xem dữ liệu CDC
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ecommerce_batch_topic \
  --from-beginning
```

## Cấu hình Connector

### Transformations

Connector được cấu hình với các transformations:

1. **ExtractNewRecordState (unwrap)**: Extract chỉ dữ liệu mới từ Debezium change event
2. **RegexRouter**: Route messages từ topic `ecommerce_db_server.public.ecommerce_events` sang `ecommerce_batch_topic`

### Format dữ liệu output

Dữ liệu trong Kafka topic `ecommerce_batch_topic` sẽ có format JSON:

```json
{
  "event_time": "2024-01-15 10:30:00",
  "event_type": "view",
  "product_id": 12345,
  "category_id": 678,
  "category_code": "electronics.smartphone",
  "brand": "Apple",
  "price": 999.99,
  "user_id": 1001,
  "user_session": "session-001",
  "op": "c",
  "source.ts_ms": 1705315800000
}
```

Trong đó:
- `op`: Operation type (c=create, u=update, d=delete)
- `source.ts_ms`: Timestamp của change event

## Monitoring

### Kiểm tra logs

```bash
# Logs của KafkaConnect
kubectl logs -f deployment/debezium-connect-cluster-connect -n debezium

# Logs của connector task
kubectl logs -f <connector-pod-name> -n debezium
```

### Kiểm tra replication slot trong PostgreSQL

```bash
# Kết nối vào PostgreSQL
kubectl exec -it <postgres-pod> -n database -- \
  psql -U ecommerce_user -d ecommerce_db

# Kiểm tra replication slots
SELECT * FROM pg_replication_slots;

# Kiểm tra publications
SELECT * FROM pg_publication;
```

### Kiểm tra metrics

Strimzi tự động expose metrics cho KafkaConnect. Bạn có thể truy cập qua:

```bash
# Port-forward metrics endpoint
kubectl port-forward svc/debezium-connect-cluster-connect-metrics -n debezium 9404:9404

# Xem metrics
curl http://localhost:9404/metrics
```

## Troubleshooting

### KafkaConnect không start

1. Kiểm tra Strimzi Operator đang chạy:
   ```bash
   kubectl get pods -n kafka -l name=strimzi-cluster-operator
   ```

2. Kiểm tra Kafka cluster đã sẵn sàng:
   ```bash
   kubectl get kafka kafka-cluster -n kafka
   ```

3. Kiểm tra logs của KafkaConnect:
   ```bash
   kubectl logs -f deployment/debezium-connect-cluster-connect -n debezium
   ```

### Connector không start

1. Kiểm tra connector status:
   ```bash
   kubectl describe kafkaconnector ecommerce-postgres-connector -n debezium
   ```

2. Kiểm tra PostgreSQL connection:
   - Đảm bảo PostgreSQL đã enable logical replication (`wal_level=logical`)
   - Kiểm tra user `debezium_user` có quyền replication
   - Kiểm tra network connectivity từ debezium namespace đến database namespace

3. Kiểm tra Kafka topics:
   ```bash
   kubectl exec -it kafka-cluster-kafka-0 -n kafka -- \
     bin/kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

### Connector bị FAILED

1. Xem chi tiết lỗi:
   ```bash
   kubectl get kafkaconnector ecommerce-postgres-connector -n debezium -o yaml
   ```

2. Kiểm tra replication slot:
   ```sql
   SELECT * FROM pg_replication_slots WHERE slot_name = 'debezium_slot';
   ```

3. Xóa và tạo lại connector:
   ```bash
   kubectl delete kafkaconnector ecommerce-postgres-connector -n debezium
   # Xóa replication slot nếu cần
   kubectl exec -it <postgres-pod> -n database -- \
     psql -U ecommerce_user -d ecommerce_db -c "SELECT pg_drop_replication_slot('debezium_slot');"
   # Tạo lại
   kubectl apply -f ./kubernetes/debezium/kafkaconnector.yaml
   ```

## Cập nhật Connector

Để cập nhật cấu hình connector:

```bash
# Sửa file kafkaconnector.yaml
# Sau đó apply lại
kubectl apply -f ./kubernetes/debezium/kafkaconnector.yaml

# Strimzi sẽ tự động cập nhật connector
```

## Cleanup

```bash
# Xóa connector
kubectl delete kafkaconnector ecommerce-postgres-connector -n debezium

# Xóa KafkaConnect cluster
kubectl delete kafkaconnect debezium-connect-cluster -n debezium

# Xóa các resources khác
kubectl delete -f ./kubernetes/debezium/secret.yaml
kubectl delete -f ./kubernetes/debezium/rbac.yaml

# Xóa namespace
kubectl delete namespace debezium
```

## Lợi ích của Strimzi

1. **Quản lý tự động**: Strimzi tự động quản lý pods, services, configs
2. **High Availability**: Dễ dàng scale KafkaConnect cluster
3. **Declarative**: Sử dụng CRDs, dễ quản lý với GitOps
4. **Monitoring**: Tích hợp sẵn metrics và health checks
5. **Security**: Hỗ trợ TLS, authentication, authorization
6. **Rolling Updates**: Tự động rolling update khi có thay đổi

## Tài liệu tham khảo

- [Strimzi KafkaConnect Documentation](https://strimzi.io/docs/operators/latest/full/deploying.html#proc-kafka-connect-str)
- [Strimzi KafkaConnector Documentation](https://strimzi.io/docs/operators/latest/full/deploying.html#proc-kafka-connector-str)
- [Debezium PostgreSQL Connector Documentation](https://debezium.io/documentation/reference/connectors/postgresql.html)
- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
