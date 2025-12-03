# Hướng Dẫn CDC từ Postgres sang Kafka với Debezium

## Tổng Quan

Hướng dẫn này mô tả cách thiết lập Change Data Capture (CDC) từ PostgreSQL sang Apache Kafka sử dụng Debezium trong môi trường Docker Compose. Debezium sẽ tự động capture các thay đổi (INSERT, UPDATE, DELETE) trong PostgreSQL và gửi chúng đến Kafka topics.

## Kiến Trúc

```
PostgreSQL (Source Database)
    ↓ (Logical Replication)
Debezium Connector (Kafka Connect)
    ↓ (CDC Events)
Apache Kafka
    ↓
Spark Streaming/Batch Processing
    ↓
ClickHouse / MinIO (Data Lake)
```

## Quick Start (Khởi Động Nhanh)

Nếu bạn muốn thiết lập nhanh, sử dụng script tự động:

### Windows (PowerShell):
```powershell
.\setup-cdc.ps1
```

### Linux/Mac (Bash):
```bash
chmod +x setup-cdc.sh
./setup-cdc.sh
```

Script sẽ tự động:
1. Khởi động Docker Compose services
2. Thiết lập database schema và publication
3. Tạo Debezium connector
4. Kiểm tra trạng thái

Sau khi script chạy xong, bạn có thể:
- Kiểm tra Kafka UI tại: http://localhost:8083
- Xem messages trong topic: `ecommerce_server.public.ecommerce_events`
- Thêm dữ liệu vào PostgreSQL để test CDC

## Yêu Cầu Hệ Thống

- Docker và Docker Compose đã được cài đặt
- Tối thiểu 4GB RAM
- Ports cần mở: 5432, 9092, 8083, 8084
- jq (cho script bash) - tùy chọn, để format JSON output

## Cấu Trúc Docker Compose

Dự án đã có sẵn các services cần thiết trong `docker-compose.yaml`:

1. **PostgreSQL** (debezium/postgres:16) - Database nguồn với logical replication enabled
2. **Kafka** (confluentinc/cp-kafka:7.5.0) - Message broker
3. **Kafka Connect** (debezium/connect:2.6) - Debezium connector runtime
4. **Kafka UI** - Giao diện quản lý Kafka

## Bước 1: Khởi Động Các Services

```bash
# Khởi động tất cả services
docker-compose up -d

# Kiểm tra trạng thái các containers
docker-compose ps

# Xem logs của các services
docker-compose logs -f kafka-connect
docker-compose logs -f postgres
docker-compose logs -f kafka
```

## Bước 2: Tạo Database Schema trong PostgreSQL

Kết nối đến PostgreSQL và tạo bảng `ecommerce_events`:

```bash
# Kết nối đến PostgreSQL container
docker exec -it postgres psql -U ecommerce_user -d ecommerce
```

Trong PostgreSQL shell, tạo bảng:

```sql
-- Tạo bảng ecommerce_events
CREATE TABLE ecommerce_events (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type VARCHAR(50) NOT NULL,
    product_id BIGINT NOT NULL,
    category_id BIGINT,
    category_code VARCHAR(255),
    brand VARCHAR(255),
    price DECIMAL(10, 2),
    user_id BIGINT NOT NULL,
    user_session VARCHAR(255)
);

-- Tạo index để tối ưu hiệu suất
CREATE INDEX idx_event_time ON ecommerce_events(event_time);
CREATE INDEX idx_user_id ON ecommerce_events(user_id);
CREATE INDEX idx_product_id ON ecommerce_events(product_id);

-- Kiểm tra bảng đã được tạo
\dt
```

## Bước 3: Kích Hoạt Logical Replication trong PostgreSQL

PostgreSQL image `debezium/postgres:16` đã được cấu hình sẵn với logical replication. Tuy nhiên, bạn cần đảm bảo các tham số sau được bật:

```sql
-- Kiểm tra cấu hình replication
SHOW wal_level;
-- Kết quả mong đợi: logical

SHOW max_replication_slots;
-- Kết quả mong đợi: >= 1

SHOW max_wal_senders;
-- Kết quả mong đợi: >= 1
```

Nếu cần cấu hình thủ công, thêm vào `postgresql.conf`:

```properties
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

## Bước 4: Tạo Publication trong PostgreSQL

Publication cho phép Debezium subscribe vào các thay đổi của bảng:

```sql
-- Tạo publication cho tất cả các bảng trong schema public
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Hoặc chỉ cho bảng cụ thể
CREATE PUBLICATION dbz_publication FOR TABLE ecommerce_events;

-- Kiểm tra publication
SELECT * FROM pg_publication;
```

## Bước 5: Tạo Debezium Connector

Sử dụng Kafka Connect REST API để tạo connector. Tạo file `debezium-connector.json`:

```json
{
  "name": "ecommerce-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "ecommerce_user",
    "database.password": "ecommerce_password",
    "database.dbname": "ecommerce",
    "database.server.name": "ecommerce_server",
    "table.include.list": "public.ecommerce_events",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "disabled",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,source.ts_ms"
  }
}
```

Tạo connector bằng curl:

```bash
# Đợi Kafka Connect sẵn sàng (khoảng 30-60 giây sau khi start)
sleep 60

# Tạo connector
curl -X POST http://localhost:8084/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-connector.json

# Kiểm tra trạng thái connector
curl http://localhost:8084/connectors/ecommerce-postgres-connector/status

# Xem danh sách connectors
curl http://localhost:8084/connectors
```

## Bước 6: Kiểm Tra Kafka Topics

Debezium sẽ tự động tạo topic với format: `<database.server.name>.<schema>.<table>`

```bash
# Liệt kê các topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --list

# Xem thông tin topic cụ thể
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 \
  --describe --topic ecommerce_server.public.ecommerce_events

# Consume messages từ topic để kiểm tra
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic ecommerce_server.public.ecommerce_events \
  --from-beginning
```

Hoặc sử dụng Kafka UI tại: http://localhost:8083

## Bước 7: Test CDC với Dữ Liệu Mẫu

Chèn dữ liệu test vào PostgreSQL:

```sql
-- Kết nối lại PostgreSQL
docker exec -it postgres psql -U ecommerce_user -d ecommerce

-- Insert dữ liệu test
INSERT INTO ecommerce_events (
    event_time, event_type, product_id, category_id, 
    category_code, brand, price, user_id, user_session
) VALUES 
('2024-01-15 10:30:00', 'view', 12345, 1, 'electronics.smartphone', 'Samsung', 899.99, 1001, 'session_abc123'),
('2024-01-15 10:31:00', 'cart', 12345, 1, 'electronics.smartphone', 'Samsung', 899.99, 1001, 'session_abc123'),
('2024-01-15 10:32:00', 'purchase', 12345, 1, 'electronics.smartphone', 'Samsung', 899.99, 1001, 'session_abc123');

-- Update dữ liệu
UPDATE ecommerce_events 
SET price = 799.99 
WHERE product_id = 12345 AND event_type = 'purchase';

-- Delete dữ liệu (tùy chọn)
DELETE FROM ecommerce_events WHERE id = 1;
```

Kiểm tra messages trong Kafka topic để xác nhận CDC hoạt động.

## Bước 8: Format của CDC Events trong Kafka

Debezium tạo ra các events với format JSON:

### INSERT Event:
```json
{
  "before": null,
  "after": {
    "id": 1,
    "event_time": 1705312200000,
    "event_type": "view",
    "product_id": 12345,
    "category_id": 1,
    "category_code": "electronics.smartphone",
    "brand": "Samsung",
    "price": 899.99,
    "user_id": 1001,
    "user_session": "session_abc123"
  },
  "source": {
    "version": "2.6.0",
    "connector": "postgresql",
    "name": "ecommerce_server",
    "ts_ms": 1705312200000,
    "snapshot": "false",
    "db": "ecommerce",
    "sequence": "[\"0:1705312200000:0\",\"0:1705312200000:0\"]",
    "schema": "public",
    "table": "ecommerce_events",
    "txId": 1234,
    "lsid": null,
    "xmin": null
  },
  "op": "c",
  "ts_ms": 1705312200000
}
```

### UPDATE Event:
```json
{
  "before": {
    "id": 1,
    "price": 899.99
  },
  "after": {
    "id": 1,
    "price": 799.99
  },
  "source": {...},
  "op": "u",
  "ts_ms": 1705312200000
}
```

### DELETE Event:
```json
{
  "before": {
    "id": 1,
    ...
  },
  "after": null,
  "source": {...},
  "op": "d",
  "ts_ms": 1705312200000
}
```

**Op Codes:**
- `c` = CREATE (INSERT)
- `u` = UPDATE
- `d` = DELETE
- `r` = READ (snapshot)

## Bước 9: Xử Lý CDC Events với Spark

Dự án đã có sẵn code xử lý trong `BATCH.scala` và `STREAMING.scala`:

### Batch Processing:
- Đọc từ Kafka topic: `ecommerce_server.public.ecommerce_events`
- Parse CDC events thành `EcommerceEvent`
- Lưu vào MinIO (Delta Lake format)

### Streaming Processing:
- Stream processing từ Kafka
- Ghi trực tiếp vào ClickHouse

## Cấu Hình Nâng Cao

### 1. Filter Tables
Chỉ capture một số bảng cụ thể:

```json
"table.include.list": "public.ecommerce_events,public.products"
```

Hoặc exclude một số bảng:

```json
"table.exclude.list": "public.temp_table"
```

### 2. Capture Specific Columns
Chỉ capture một số cột:

```json
"column.include.list": "public.ecommerce_events.event_type,public.ecommerce_events.product_id"
```

### 3. Snapshot Mode
Cấu hình snapshot behavior:

```json
"snapshot.mode": "initial"  // initial, never, always, exported
```

- `initial`: Chụp snapshot ban đầu rồi tiếp tục CDC
- `never`: Chỉ CDC, không snapshot
- `always`: Luôn snapshot khi connector start
- `exported`: Sử dụng snapshot đã export

### 4. Topic Naming
Tùy chỉnh tên topic:

```json
"topic.prefix": "ecommerce_server",
"topic.naming.strategy": "io.debezium.schema.SchemaTopicNamingStrategy"
```

### 5. Error Handling
Cấu hình xử lý lỗi:

```json
"errors.tolerance": "all",
"errors.log.enable": "true",
"errors.log.include.messages": "true"
```

## Monitoring và Troubleshooting

### Kiểm Tra Connector Status

```bash
# Xem status chi tiết
curl http://localhost:8084/connectors/ecommerce-postgres-connector/status | jq

# Xem config của connector
curl http://localhost:8084/connectors/ecommerce-postgres-connector/config | jq

# Xem tasks của connector
curl http://localhost:8084/connectors/ecommerce-postgres-connector/tasks | jq
```

### Kiểm Tra Replication Slots

```sql
-- Xem replication slots
SELECT * FROM pg_replication_slots;

-- Xem thông tin chi tiết
SELECT 
    slot_name,
    plugin,
    slot_type,
    database,
    active,
    restart_lsn
FROM pg_replication_slots;
```

### Xem Logs

```bash
# Logs của Kafka Connect
docker-compose logs -f kafka-connect

# Logs của PostgreSQL
docker-compose logs -f postgres

# Logs của Kafka
docker-compose logs -f kafka
```

### Các Lỗi Thường Gặp

#### 1. Connector FAILED
**Nguyên nhân:** Không kết nối được PostgreSQL hoặc thiếu publication
**Giải pháp:**
```sql
-- Kiểm tra publication tồn tại
SELECT * FROM pg_publication;

-- Tạo lại nếu thiếu
CREATE PUBLICATION dbz_publication FOR TABLE ecommerce_events;
```

#### 2. Replication Slot Already Exists
**Nguyên nhân:** Slot đã tồn tại từ lần chạy trước
**Giải pháp:**
```sql
-- Xóa slot cũ
SELECT pg_drop_replication_slot('debezium_slot');

-- Hoặc đổi tên slot trong connector config
```

#### 3. WAL Files Accumulation
**Nguyên nhân:** Consumer không đọc messages, WAL files tích tụ
**Giải pháp:**
```sql
-- Kiểm tra WAL size
SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) 
FROM pg_replication_slots 
WHERE slot_name = 'debezium_slot';

-- Nếu quá lớn, cần consumer messages từ Kafka
```

#### 4. Schema Registry Issues
**Nguyên nhân:** Thiếu Schema Registry (nếu dùng Avro)
**Giải pháp:** Sử dụng JSON converter (đã cấu hình sẵn) hoặc thêm Schema Registry service

## Dọn Dẹp và Khởi Động Lại

```bash
# Dừng tất cả services
docker-compose down

# Xóa volumes (cẩn thận: mất dữ liệu)
docker-compose down -v

# Xóa replication slot trước khi xóa volume
docker exec -it postgres psql -U ecommerce_user -d ecommerce -c \
  "SELECT pg_drop_replication_slot('debezium_slot');"

# Khởi động lại
docker-compose up -d
```

## Tài Liệu Tham Khảo

- [Debezium PostgreSQL Connector Documentation](https://debezium.io/documentation/reference/connectors/postgresql.html)
- [Kafka Connect REST API](https://kafka.apache.org/documentation/#connect_rest)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)

## Kết Luận

Sau khi hoàn thành các bước trên, bạn đã thiết lập thành công CDC từ PostgreSQL sang Kafka với Debezium. Tất cả các thay đổi trong bảng `ecommerce_events` sẽ được tự động capture và gửi đến Kafka topic, sẵn sàng cho Spark streaming/batch processing.

