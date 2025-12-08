#!/bin/bash

# Script để triển khai Debezium PostgreSQL Connector với Strimzi
# Usage: ./register-connector.sh
# 
# Note: Với Strimzi, connector được đăng ký tự động qua KafkaConnector CRD
# Script này chỉ apply KafkaConnector resource

set -e

DEBEZIUM_NAMESPACE="kafka"
CONNECTOR_FILE="./kubernetes/debezium/kafkaconnector.yaml"

echo "Checking if KafkaConnect cluster is running..."
if ! kubectl get kafkaconnect debezium-connect-cluster -n $DEBEZIUM_NAMESPACE &>/dev/null; then
    echo "Error: KafkaConnect cluster not found in namespace $DEBEZIUM_NAMESPACE"
    echo "Please deploy KafkaConnect first:"
    echo "  kubectl apply -f ./kubernetes/debezium/kafkaconnect.yaml"
    exit 1
fi

echo "Checking KafkaConnect cluster status..."
kubectl wait --for=condition=Ready kafkaconnect/debezium-connect-cluster -n $DEBEZIUM_NAMESPACE --timeout=60s || {
    echo "Error: KafkaConnect cluster is not ready"
    exit 1
}

echo "Applying KafkaConnector resource..."
kubectl apply -f $CONNECTOR_FILE

echo ""
echo "Waiting for connector to be ready..."
sleep 5

echo ""
echo "=== Connector Status ==="
kubectl get kafkaconnector ecommerce-postgres-connector -n $DEBEZIUM_NAMESPACE

echo ""
echo "=== Connector Details ==="
kubectl describe kafkaconnector ecommerce-postgres-connector -n $DEBEZIUM_NAMESPACE | tail -30

echo ""
echo "✓ Connector deployed successfully!"
echo ""
echo "To check status later, run:"
echo "  kubectl get kafkaconnector ecommerce-postgres-connector -n $DEBEZIUM_NAMESPACE"
echo "  kubectl describe kafkaconnector ecommerce-postgres-connector -n $DEBEZIUM_NAMESPACE"
echo ""
echo "To check via REST API:"
echo "  kubectl port-forward svc/debezium-connect-cluster-connect-api 8083:8083 -n $DEBEZIUM_NAMESPACE"
echo "  curl http://localhost:8083/connectors/ecommerce-postgres-connector/status"

