# Ecommerce PostgreSQL Database Setup

PostgreSQL database được sử dụng làm nguồn dữ liệu cho Debezium CDC.

## Tổng quan

Database này chứa:
- Bảng `ecommerce_events`: Lưu trữ các events từ hệ thống ecommerce
- User `debezium_user`: User được sử dụng bởi Debezium để thực hiện CDC
- Logical replication: Đã được enable để hỗ trợ Debezium

## Yêu cầu

- Helm 3+ đã cài đặt
- kubectl đã cấu hình
- Bitnami PostgreSQL Helm repository đã được add

## Triển khai

### 1. Thêm Bitnami Helm repository

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```

### 2. Tạo namespace

```bash
kubectl create namespace database
```

### 3. Áp dụng Secret

```bash
kubectl apply -f ./kubernetes/database/secret.yaml
```

### 4. Triển khai PostgreSQL

```bash
helm install database bitnami/postgresql \
  --namespace database \
  -f ./kubernetes/database/values.yaml
```

### 5. Kiểm tra deployment

```bash
# Kiểm tra pods
kubectl get pods -n database

# Kiểm tra services
kubectl get svc -n database

# Kiểm tra logs
kubectl logs -f statefulset/database-postgresql -n database
```

### 6. Chạy init script (nếu cần)

Nếu init script không tự động chạy, bạn có thể chạy thủ công:

```bash
# Port-forward PostgreSQL
kubectl port-forward svc/database-postgresql -n database 5432:5432

# Trong terminal khác, chạy init script
PGPASSWORD=ecommerce_password psql -h localhost -U ecommerce_user -d ecommerce_db -f ./kubernetes/database/init-script.sql
```

Hoặc exec vào pod:

```bash
kubectl exec -it database-postgresql-0 -n database -- \
  psql -U ecommerce_user -d ecommerce_db -f /path/to/init-script.sql
```

## Kiểm tra Logical Replication

```bash
# Kết nối vào PostgreSQL
kubectl exec -it database-postgresql-0 -n database -- \
  psql -U ecommerce_user -d ecommerce_db

# Kiểm tra wal_level
SHOW wal_level; -- Phải là 'logical'

# Kiểm tra max_wal_senders
SHOW max_wal_senders; -- Phải >= 1

# Kiểm tra max_replication_slots
SHOW max_replication_slots; -- Phải >= 1

# Kiểm tra replication user
SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'debezium_user';
```

## Thêm dữ liệu test

```bash
# Kết nối vào PostgreSQL
kubectl exec -it database-postgresql-0 -n database -- \
  psql -U ecommerce_user -d ecommerce_db

# Thêm dữ liệu test
INSERT INTO ecommerce_events (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session)
VALUES 
  (NOW(), 'view', 12345, 678, 'electronics.smartphone', 'Apple', 999.99, 1001, 'session-001'),
  (NOW(), 'cart', 12346, 678, 'electronics.smartphone', 'Samsung', 899.99, 1002, 'session-002'),
  (NOW(), 'purchase', 12347, 679, 'electronics.tablet', 'iPad', 599.99, 1001, 'session-001');
```

## Connection String

- **Host**: `database-postgresql.database.svc.cluster.local`
- **Port**: `5432`
- **Database**: `ecommerce_db`
- **User**: `ecommerce_user`
- **Password**: `ecommerce_password`

## Credentials

Tất cả credentials được lưu trong Secret `database-secret`:
- `postgres-username`: ecommerce_user
- `postgres-password`: ecommerce_password
- `postgres-database`: ecommerce_db
- `debezium-username`: debezium_user
- `debezium-password`: debezium_password

## Schema

### Table: ecommerce_events

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| event_time | TIMESTAMP | Thời gian event xảy ra |
| event_type | VARCHAR(50) | Loại event (view, cart, purchase, etc.) |
| product_id | BIGINT | ID sản phẩm |
| category_id | BIGINT | ID category |
| category_code | VARCHAR(255) | Mã category |
| brand | VARCHAR(255) | Thương hiệu |
| price | DECIMAL(10,2) | Giá sản phẩm |
| user_id | BIGINT | ID người dùng |
| user_session | VARCHAR(255) | Session ID |
| created_at | TIMESTAMP | Thời gian tạo record |
| updated_at | TIMESTAMP | Thời gian cập nhật cuối |

## Cleanup

```bash
# Uninstall PostgreSQL
helm uninstall database -n database

# Xóa namespace (sẽ xóa tất cả resources)
kubectl delete namespace database
```

## Troubleshooting

### Logical replication không hoạt động

1. Kiểm tra PostgreSQL configuration:
   ```sql
   SHOW wal_level;
   SHOW max_wal_senders;
   SHOW max_replication_slots;
   ```

2. Nếu `wal_level` không phải là `logical`, cần restart PostgreSQL với config đúng trong `values.yaml`.

### Không thể tạo replication slot

1. Kiểm tra user `debezium_user` có quyền replication:
   ```sql
   SELECT rolreplication FROM pg_roles WHERE rolname = 'debezium_user';
   ```

2. Kiểm tra số lượng replication slots còn trống:
   ```sql
   SELECT count(*) FROM pg_replication_slots;
   SELECT max_replication_slots FROM pg_settings WHERE name = 'max_replication_slots';
   ```





** Please be patient while the chart is being deployed **

PostgreSQL can be accessed via port 5432 on the following DNS names from within your cluster:

    database-postgresql.database.svc.cluster.local - Read/Write connection

To get the password for "postgres" run:

    export POSTGRES_ADMIN_PASSWORD=$(kubectl get secret --namespace database database-postgresql -o jsonpath="{.data.postgres-password}" | base64 -d)

To get the password for "ecommerce_user" run:

    export POSTGRES_PASSWORD=$(kubectl get secret --namespace database database-postgresql -o jsonpath="{.data.password}" | base64 -d)

To connect to your database run the following command:

    kubectl run database-postgresql-client --rm --tty -i --restart='Never' --namespace database --image registry-1.docker.io/bitnami/postgresql:15.5.0 --env="PGPASSWORD=$POSTGRES_PASSWORD" \
      --command -- psql --host database-postgresql -U ecommerce_user -d ecommerce_db -p 5432

    > NOTE: If you access the container using bash, make sure that you execute "/opt/bitnami/scripts/postgresql/entrypoint.sh /bin/bash" in order to avoid the error "psql: local user with ID 1001} does not exist"

To connect to your database from outside the cluster execute the following commands:

    kubectl port-forward --namespace database svc/database-postgresql 5432:5432 &
    PGPASSWORD="$POSTGRES_PASSWORD" psql --host 127.0.0.1 -U ecommerce_user -d ecommerce_db -p 5432
