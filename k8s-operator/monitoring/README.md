Monitoring Stack (Prometheus & Grafana)
===================================================

This guide explains how to deploy a monitoring stack (Prometheus + Grafana)
using the `kube-prometheus-stack` Helm chart, and how to add a Kafka exporter
to scrape Kafka metrics in this project.

> All commands assume:
> - You have a running Minikube cluster (`minikube start`).
> - `kubectl` is configured to point to Minikube.
> - `helm` v3+ is installed.
> - You run commands from the project root (`ecommerce-behavior-analytics`).


1. Enable Minikube metrics-server and create namespace
------------------------------------------------------

Enable the built-in `metrics-server` add-on:

```bash
minikube addons enable metrics-server
```

Create a dedicated namespace for monitoring components:

```bash
kubectl create namespace monitoring
```


2. Add and update the Prometheus Helm repository
------------------------------------------------

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

Verify the chart is available:

```bash
helm search repo prometheus-community/kube-prometheus-stack
```


3. Install kube-prometheus-stack (Prometheus + Grafana)
-------------------------------------------------------

Install the stack into the `monitoring` namespace:

```bash
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring
```

Check that the pods are up and running:

```bash
kubectl get pods -n monitoring
kubectl get svc -n monitoring
```


4. Access Grafana and Prometheus
--------------------------------

Get the Grafana admin password:

```bash
kubectl get secret -n monitoring prometheus-grafana \
  -o jsonpath="{.data.admin-password}" | base64 --decode
echo
```

Access Grafana via port-forward:

```bash
kubectl --namespace monitoring port-forward svc/prometheus-grafana 3000:80
```

Then open `http://localhost:3000` in your browser and log in with:

- **Username**: `admin`
- **Password**: value from the command above

Alternatively, you can use Minikube’s `service` command:

```bash
minikube service prometheus-grafana -n monitoring
```

To access Prometheus directly:

```bash
kubectl --namespace monitoring port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090
```

You can then open `http://localhost:9090` in your browser.

If you need the Prometheus admin password (for some deployments):

```bash
kubectl get secret --namespace monitoring \
  -l app.kubernetes.io/component=admin-secret \
  -o jsonpath="{.items[0].data.admin-password}" | base64 --decode
echo
```


5. Customize the monitoring stack
---------------------------------

You can customize the stack using the provided `values.yaml` in this folder.
To apply your custom settings:

```bash
helm upgrade prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  -f values.yaml
```

Verify that the changes are applied by checking the relevant resources:

```bash
kubectl get pods,svc -n monitoring
```


6. Install Kafka exporter for Kafka metrics
------------------------------------------

To collect Kafka metrics from your Kafka cluster (installed via Strimzi),
install the Kafka exporter chart. From the project root:

```bash
helm install kafka-exporter prometheus-community/prometheus-kafka-exporter \
  -n kafka \
  -f .\kubernetes\monitoring\exporter\kafka-exporter.yaml
```

(For Linux/macOS shells, use `./kubernetes/monitoring/exporter/kafka-exporter.yaml`.)

Verify that the exporter is running:

```bash
kubectl get pods -n kafka
kubectl get svc -n kafka
```

You should see a service that is scraped by Prometheus; Kafka-related Grafana
dashboards can then be configured to use these metrics.


7. Cleanup and references
-------------------------

- Remove the monitoring stack:

  ```bash
  helm uninstall prometheus -n monitoring
  kubectl delete namespace monitoring
  ```

- Remove the Kafka exporter (in the `kafka` namespace):

  ```bash
  helm uninstall kafka-exporter -n kafka
  ```

- Official docs and resources:
  - kube-prometheus-stack: `https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack`
  - Prometheus documentation: `https://prometheus.io/docs/introduction/overview/`
  - Grafana documentation: `https://grafana.com/docs/`