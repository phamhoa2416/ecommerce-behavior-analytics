ClickHouse Operator Installation (Altinity Helm Chart)
=====================================================

This guide explains how to install the Altinity ClickHouse Operator on a Minikube
cluster and create a ClickHouse installation suitable for the ecommerce events
workload in this project.

> All commands assume:
> - You have a running Minikube cluster (`minikube start`).
> - `kubectl` is configured to point to Minikube.
> - `helm` v3+ is installed.
> - You run commands from the project root (`ecommerce-behavior-analytics`).


1. Add and update the Altinity Helm repository
----------------------------------------------

```bash
helm repo add altinity https://helm.altinity.com
helm repo update
```

To verify the repo was added:

```bash
helm search repo altinity
```


2. Install the ClickHouse Operator
----------------------------------

Create (or reuse) the `clickhouse` namespace and install the operator:

```bash
helm install clickhouse-operator altinity/altinity-clickhouse-operator \
  --namespace clickhouse \
  --create-namespace
```

Check that the operator pods are running:

```bash
kubectl get pods -n clickhouse
kubectl get deployment -n clickhouse
```


3. Deploy the ClickHouse cluster for this project
-------------------------------------------------

Apply the ClickHouseInstallation manifest provided by this repository.
From the project root:

```bash
kubectl apply -f .\kubernetes\clickhouse\clickhouse-values.yaml -n clickhouse
```

(For Linux/macOS shells, use `./kubernetes/clickhouse/clickhouse-values.yaml`.)

Validate the ClickHouseInstallation and pods:

```bash
kubectl get clickhouseinstallation -n clickhouse
kubectl get pods -n clickhouse
```

Wait until all ClickHouse pods show `STATUS=Running`.


4. Connect to ClickHouse and create the ecommerce schema
--------------------------------------------------------

Open a shell inside one of the ClickHouse pods (update the pod name if needed,
you can get the exact name from `kubectl get pods -n clickhouse`):

```bash
kubectl exec -it chi-clickhouse-clickhouse-0-0-0 -n clickhouse -- /bin/bash
```

Inside the container, connect with the ClickHouse client (adjust user/password
if you changed them in the manifest):

```bash
clickhouse-client --user=admin --password=password
```

Then run the following SQL to create the target database and table:

```sql
SHOW DATABASES;

CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

CREATE TABLE IF NOT EXISTS ecommerce_events
(
    event_time    DateTime,
    event_type    String,
    product_id    UInt64,
    category_id   UInt64,
    category_code String,
    brand         String,
    price         Float64,
    user_id       UInt64,
    user_session  String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, user_id);
```

You can verify that the table exists:

```sql
SHOW TABLES FROM ecommerce;
DESCRIBE TABLE ecommerce.ecommerce_events;
```

Type `QUIT` or press `Ctrl+D` to exit the ClickHouse client, then exit the
container shell.


5. Useful commands and references
---------------------------------

- Check operator logs (useful for debugging):

  ```bash
  kubectl logs deploy/clickhouse-operator -n clickhouse
  ```

- Delete the ClickHouse installation (but keep the operator):

  ```bash
  kubectl delete -f .\kubernetes\clickhouse\clickhouse-values.yaml -n clickhouse
  ```

- Remove everything (operator + installation):

  ```bash
  helm uninstall clickhouse-operator -n clickhouse
  kubectl delete namespace clickhouse
  ```

- Official docs:
  - Altinity ClickHouse Operator: `https://github.com/Altinity/clickhouse-operator`
  - ClickHouse documentation: `https://clickhouse.com/docs`
