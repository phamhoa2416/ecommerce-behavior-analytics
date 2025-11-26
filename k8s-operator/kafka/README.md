Kafka Installation with Strimzi Operator
=======================================

This guide explains how to install the Strimzi Kafka Operator on a Minikube
cluster and deploy a Kafka cluster configuration for this project.

> All commands assume:
> - You have a running Minikube cluster (`minikube start`).
> - `kubectl` is configured to point to Minikube.
> - `helm` v3+ is installed.
> - You run commands from the project root (`ecommerce-behavior-analytics`).


1. Add and update the Strimzi Helm repository
---------------------------------------------

```bash
helm repo add strimzi https://strimzi.io/charts/
helm repo update
```

Verify that the chart is available:

```bash
helm search repo strimzi
```


2. Create the Kafka namespace and install the Strimzi operator
--------------------------------------------------------------

Create a dedicated namespace:

```bash
kubectl create namespace kafka
```

Install the Strimzi Kafka Operator into the `kafka` namespace:

```bash
helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator -n kafka
```

Check that the operator is running:

```bash
kubectl get pods -n kafka
kubectl get deployment -n kafka
```


3. Deploy the Kafka cluster for this project
-------------------------------------------

Apply the Kafka custom resource definition (CR) provided in this repository.
From the project root:

```bash
kubectl apply -f .\kubernetes\kafka\kafka-values.yaml -n kafka
```

(For Linux/macOS shells, use `./kubernetes/kafka/kafka-values.yaml`.)

Verify the Kafka resources:

```bash
kubectl get kafka -n kafka
kubectl get pods -n kafka
kubectl get svc -n kafka
```

Wait until all Kafka pods are in `Running` state.


4. Basic connectivity test
--------------------------

Use Strimzi’s built-in test clients to verify the cluster (pod names may differ;
check them with `kubectl get pods -n kafka`):

```bash
kubectl run kafka-producer -ti --image=quay.io/strimzi/kafka:latest-kafka-3.7.0 \
  -n kafka --rm=true --restart=Never -- \
  bin/kafka-console-producer.sh --broker-list my-cluster-kafka-bootstrap:9092 --topic test
```

In another terminal:

```bash
kubectl run kafka-consumer -ti --image=quay.io/strimzi/kafka:latest-kafka-3.7.0 \
  -n kafka --rm=true --restart=Never -- \
  bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 \
  --topic test --from-beginning
```

Type a few messages in the producer terminal and confirm they appear in the
consumer terminal.


5. Cleanup and references
-------------------------

- Delete the Kafka cluster (keep operator):

  ```bash
  kubectl delete -f .\kubernetes\kafka\kafka-values.yaml -n kafka
  ```

- Remove the operator and namespace:

  ```bash
  helm uninstall strimzi-kafka-operator -n kafka
  kubectl delete namespace kafka
  ```

- Official docs and resources:
  - Strimzi documentation: `https://strimzi.io/documentation/`
  - Strimzi Helm charts: `https://strimzi.io/charts/`
  - Apache Kafka documentation: `https://kafka.apache.org/documentation/`

