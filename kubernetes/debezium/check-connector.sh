#!/bin/bash

# Script để kiểm tra status của Debezium connector (Strimzi)
# Usage: ./check-connector.sh

set -e

DEBEZIUM_SERVICE="debezium-connect-cluster-connect-api"
DEBEZIUM_NAMESPACE="kafka"
DEBEZIUM_PORT="8083"
CONNECTOR_NAME="ecommerce-postgres-connector"

echo "Checking Debezium Connect status (Strimzi)..."

# Check if KafkaConnect exists
if ! kubectl get kafkaconnect debezium-connect-cluster -n $DEBEZIUM_NAMESPACE &>/dev/null; then
    echo "Error: KafkaConnect cluster not found"
    exit 1
fi

# Check if service exists
if ! kubectl get svc $DEBEZIUM_SERVICE -n $DEBEZIUM_NAMESPACE &>/dev/null; then
    echo "Error: Debezium Connect service not found"
    echo "Waiting for service to be created..."
    sleep 5
fi

# Port-forward
echo "Port-forwarding Debezium Connect service..."
kubectl port-forward svc/$DEBEZIUM_SERVICE $DEBEZIUM_PORT:$DEBEZIUM_PORT -n $DEBEZIUM_NAMESPACE &
PF_PID=$!

# Wait for port-forward
sleep 3

echo ""
echo "=== KafkaConnect Status ==="
kubectl get kafkaconnect debezium-connect-cluster -n $DEBEZIUM_NAMESPACE

echo ""
echo "=== KafkaConnector Status ==="
kubectl get kafkaconnector $CONNECTOR_NAME -n $DEBEZIUM_NAMESPACE

echo ""
echo "=== List of Connectors (REST API) ==="
curl -s http://localhost:$DEBEZIUM_PORT/connectors | jq . || echo "Failed to connect to REST API"

echo ""
echo "=== Connector Status (REST API) ==="
curl -s http://localhost:$DEBEZIUM_PORT/connectors/$CONNECTOR_NAME/status | jq . || echo "Failed to get connector status"

echo ""
echo "=== Connector Config (REST API) ==="
curl -s http://localhost:$DEBEZIUM_PORT/connectors/$CONNECTOR_NAME/config | jq . || echo "Failed to get connector config"

echo ""
echo "=== Connector Tasks (REST API) ==="
curl -s http://localhost:$DEBEZIUM_PORT/connectors/$CONNECTOR_NAME/tasks | jq . || echo "Failed to get connector tasks"

echo ""
echo "=== Connector Details (kubectl) ==="
kubectl describe kafkaconnector $CONNECTOR_NAME -n $DEBEZIUM_NAMESPACE | tail -20

# Cleanup
kill $PF_PID 2>/dev/null

