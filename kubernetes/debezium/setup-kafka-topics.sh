#!/bin/bash

# Script để tạo các Kafka topics cần thiết cho Debezium Connect
# Usage: ./setup-kafka-topics.sh

set -e

KAFKA_POD="kafka-cluster-kafka-0"
KAFKA_NAMESPACE="kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "Setting up Kafka topics for Debezium Connect..."

# Check if Kafka pod exists
if ! kubectl get pod $KAFKA_POD -n $KAFKA_NAMESPACE &>/dev/null; then
    echo "Error: Kafka pod $KAFKA_POD not found in namespace $KAFKA_NAMESPACE"
    exit 1
fi

# Function to create topic if it doesn't exist
create_topic() {
    local topic_name=$1
    local partitions=${2:-1}
    local replication_factor=${3:-1}
    
    echo "Creating topic: $topic_name"
    kubectl exec -it $KAFKA_POD -n $KAFKA_NAMESPACE -- bin/kafka-topics.sh \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --create \
        --topic "$topic_name" \
        --partitions "$partitions" \
        --replication-factor "$replication_factor" \
        --if-not-exists || echo "Topic $topic_name might already exist"
}

# Create Debezium Connect internal topics
create_topic "debezium-connect-configs" 1 1
create_topic "debezium-connect-offsets" 25 1
create_topic "debezium-connect-status" 5 1

# Create schema history topic
create_topic "schema-changes.ecommerce_db" 1 1

# Create target topic for CDC data (if not exists)
create_topic "ecommerce_batch_topic" 3 1

echo ""
echo "✓ All Kafka topics created successfully!"
echo ""
echo "Listing all topics:"
kubectl exec -it $KAFKA_POD -n $KAFKA_NAMESPACE -- bin/kafka-topics.sh \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --list


