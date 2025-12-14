# Hướng dẫn Build KafkaConnect với Debezium Plugin

## Vấn đề

Khi sử dụng Debezium image trực tiếp (`debezium/connect:2.6`) với Strimzi Operator, sẽ gặp lỗi:
```
/docker-entrypoint.sh: line 330: /opt/kafka/kafka_connect_run.sh: No such file or directory
```

Nguyên nhân: Debezium image không tương thích với cách Strimzi Operator khởi động Kafka Connect.

## Giải pháp

Sử dụng Strimzi build plugins để build custom image với Debezium connector.

## Cấu hình

File `kafkaconnect.yaml` đã được cấu hình với Strimzi build plugins:

```yaml
spec:
  image: quay.io/strimzi/kafka:latest-kafka-3.7.0
  build:
    output:
      type: docker
      image: debezium-connect-cluster:latest
    plugins:
      - name: debezium-postgres
        artifacts:
          - type: tgz
            url: https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.6.0.Final/debezium-connector-postgres-2.6.0.Final-plugin.tar.gz
```

## Các tùy chọn Output

### Option 1: Local Build (Không cần registry)

Sử dụng `type: docker` với image name local. Strimzi sẽ build image và lưu trong local Docker daemon.

**Yêu cầu:**
- Kubernetes cluster phải có quyền truy cập Docker daemon
- Node phải có Docker installed
- Phù hợp cho local development (Minikube, Kind, etc.)

**Cấu hình:**
```yaml
build:
  output:
    type: docker
    image: debezium-connect-cluster:latest
```

### Option 2: Container Registry (Production)

Push image lên container registry để các node có thể pull về.

**Yêu cầu:**
- Container registry (Docker Hub, Quay.io, GCR, ECR, etc.)
- Registry credentials trong Kubernetes Secret

**Bước 1: Tạo Secret với registry credentials**

```bash
# Docker Hub
kubectl create secret docker-registry docker-registry-secret \
  --docker-server=https://index.docker.io/v1/ \
  --docker-username=<your-username> \
  --docker-password=<your-password> \
  --docker-email=<your-email> \
  -n kafka

# Hoặc Quay.io
kubectl create secret docker-registry quay-registry-secret \
  --docker-server=https://quay.io \
  --docker-username=<your-username> \
  --docker-password=<your-token> \
  -n kafka
```

**Bước 2: Cập nhật kafkaconnect.yaml**

```yaml
build:
  output:
    type: docker
    image: <your-registry>/debezium-connect-cluster:latest
    pushSecret: docker-registry-secret  # Tên secret đã tạo
```

**Ví dụ với Docker Hub:**
```yaml
build:
  output:
    type: docker
    image: your-dockerhub-username/debezium-connect-cluster:latest
    pushSecret: docker-registry-secret
```

**Ví dụ với Quay.io:**
```yaml
build:
  output:
    type: docker
    image: quay.io/your-org/debezium-connect-cluster:latest
    pushSecret: quay-registry-secret
```

### Option 3: ImageStream (OpenShift)

Nếu sử dụng OpenShift, có thể dùng ImageStream:

```yaml
build:
  output:
    type: imagestream
    image: debezium-connect-cluster:latest
```

## Triển khai

### Với Local Build (Minikube/Kind)

```bash
# Đảm bảo Docker daemon accessible
# Với Minikube:
eval $(minikube docker-env)

# Apply KafkaConnect
kubectl apply -f kubernetes/debezium/kafkaconnect.yaml

# Kiểm tra build status
kubectl get kafkaconnect debezium-connect-cluster -n kafka -o yaml

# Xem build logs
kubectl logs -n kafka -l strimzi.io/kind=KafkaConnectBuild
```

### Với Container Registry

```bash
# 1. Tạo registry secret (nếu chưa có)
kubectl create secret docker-registry docker-registry-secret \
  --docker-server=https://index.docker.io/v1/ \
  --docker-username=<your-username> \
  --docker-password=<your-password> \
  -n kafka

# 2. Cập nhật image name trong kafkaconnect.yaml
# Thay đổi: image: your-registry/debezium-connect-cluster:latest

# 3. Apply
kubectl apply -f kubernetes/debezium/kafkaconnect.yaml

# 4. Kiểm tra build
kubectl get kafkaconnectbuild -n kafka
kubectl describe kafkaconnectbuild debezium-connect-cluster-build -n kafka
```

## Kiểm tra Build Status

```bash
# Kiểm tra KafkaConnect resource
kubectl get kafkaconnect debezium-connect-cluster -n kafka

# Xem chi tiết build
kubectl get kafkaconnectbuild -n kafka
kubectl describe kafkaconnectbuild debezium-connect-cluster-build -n kafka

# Xem logs của build pod
kubectl logs -n kafka -l strimzi.io/kind=KafkaConnectBuild

# Kiểm tra pods
kubectl get pods -n kafka -l strimzi.io/kind=KafkaConnect
```

## Troubleshooting

### Build không start

1. Kiểm tra Strimzi Operator đang chạy:
   ```bash
   kubectl get pods -n kafka -l name=strimzi-cluster-operator
   ```

2. Kiểm tra build resource:
   ```bash
   kubectl get kafkaconnectbuild -n kafka
   kubectl describe kafkaconnectbuild debezium-connect-cluster-build -n kafka
   ```

3. Kiểm tra events:
   ```bash
   kubectl get events -n kafka --sort-by='.lastTimestamp' | grep debezium
   ```

### Build failed

1. Kiểm tra logs:
   ```bash
   kubectl logs -n kafka -l strimzi.io/kind=KafkaConnectBuild
   ```

2. Kiểm tra network connectivity (download plugin):
   ```bash
   kubectl run -it --rm debug --image=curlimages/curl --restart=Never -- \
     curl -I https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.6.0.Final/debezium-connector-postgres-2.6.0.Final-plugin.tar.gz
   ```

3. Kiểm tra registry credentials (nếu dùng registry):
   ```bash
   kubectl get secret docker-registry-secret -n kafka -o yaml
   ```

### Image không pull được

1. Kiểm tra image đã được push:
   ```bash
   docker pull <your-registry>/debezium-connect-cluster:latest
   ```

2. Kiểm tra imagePullPolicy và image name trong pod:
   ```bash
   kubectl describe pod -n kafka -l strimzi.io/kind=KafkaConnect
   ```

## Lưu ý

1. **Build time**: Lần đầu build có thể mất 5-10 phút để download và build image
2. **Storage**: Build process cần storage để tạm thời lưu image
3. **Registry**: Nếu dùng registry, đảm bảo tất cả nodes có thể pull image
4. **Version**: Đảm bảo Kafka version trong build khớp với Kafka cluster version

## Tham khảo

- [Strimzi KafkaConnect Build Documentation](https://strimzi.io/docs/operators/latest/full/deploying.html#proc-kafka-connect-build-str)
- [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/connectors/postgresql.html)


