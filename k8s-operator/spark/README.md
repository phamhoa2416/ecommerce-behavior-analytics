Spark Operator Installation (Kubeflow Spark Operator)
====================================================

This guide explains how to install the Kubeflow Spark Operator on a Minikube
cluster and set up RBAC for running Spark applications used in this project.

> All commands assume:
> - You have a running Minikube cluster (`minikube start`).
> - `kubectl` is configured to point to Minikube.
> - `helm` v3+ is installed.
> - You run commands from the project root (`ecommerce-behavior-analytics`).


1. Add and update the Spark Operator Helm repository
----------------------------------------------------

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
```

Verify that the chart is available:

```bash
helm search repo spark-operator/spark-operator
```


2. Install the Spark Operator
-----------------------------

Install the operator into its own namespace and configure it to manage Spark
jobs running in the `default` namespace (you can change this if needed):

```bash
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set sparkJobNamespace=default \
  --set webhook.enable=true
```

Check that the operator pods are running:

```bash
kubectl get pods -n spark-operator
kubectl get deployment -n spark-operator
```


3. Configure RBAC for Spark jobs
--------------------------------

Apply the RBAC configuration so that Spark applications have the appropriate
permissions:

```bash
kubectl apply -f .\kubernetes\spark\spark-rbac.yaml
```

(For Linux/macOS shells, use `./kubernetes/spark/spark-rbac.yaml`.)

Verify the roles and role bindings:

```bash
kubectl get serviceaccount,role,rolebinding -n default
```


4. (Optional) Run sample Spark applications
-------------------------------------------

This project includes example manifests for running Spark jobs (batch and
streaming) against the ecommerce data pipeline.

- **Batch job example** (from `kubernetes/spark/batch/spark-batch.yaml`):

  ```bash
  kubectl apply -f .\kubernetes\spark\batch\spark-batch.yaml
  ```

- **Streaming job example** (from `kubernetes/spark/streaming`):

  ```bash
  kubectl apply -f .\kubernetes\spark\streaming\spark-job-configmap.yaml
  kubectl apply -f .\kubernetes\spark\streaming\spark-ecommerce-app.yaml
  ```

Check SparkApplication resources and pods:

```bash
kubectl get sparkapplications -n default
kubectl get pods -n default
```

View logs for a specific Spark driver pod:

```bash
kubectl logs <driver-pod-name> -n default
```


5. Cleanup and references
-------------------------

- Remove the sample Spark jobs:

  ```bash
  kubectl delete -f .\kubernetes\spark\batch\spark-batch.yaml --ignore-not-found
  kubectl delete -f .\kubernetes\spark\streaming\spark-ecommerce-app.yaml --ignore-not-found
  kubectl delete -f .\kubernetes\spark\streaming\spark-job-configmap.yaml --ignore-not-found
  ```

- Remove RBAC and the Spark Operator:

  ```bash
  kubectl delete -f .\kubernetes\spark\spark-rbac.yaml --ignore-not-found

  helm uninstall spark-operator -n spark-operator
  kubectl delete namespace spark-operator
  ```

- Official docs and resources:
  - Kubeflow Spark Operator: `https://github.com/kubeflow/spark-operator`
  - Spark on Kubernetes docs: `https://spark.apache.org/docs/latest/running-on-kubernetes.html`

