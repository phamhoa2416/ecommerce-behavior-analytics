MinIO Operator and Tenant on Minikube
=====================================

This guide shows how to install the MinIO Operator and create a MinIO tenant
for object storage on a Minikube cluster using the manifests in this project.

> All commands assume:
> - You have a running Minikube cluster (`minikube start`).
> - `kubectl` is configured to point to Minikube.
> - You run commands from the project root (`ecommerce-behavior-analytics`).


1. Install the MinIO Operator
-----------------------------

Install the MinIO Operator CRDs and controller using `kubectl apply -k`:

```bash
kubectl apply -k "github.com/minio/operator?ref=v7.0.1"
```

Verify that the operator pods are running (usually in the `minio-operator`
namespace):

```bash
kubectl get pods -n minio-operator
kubectl get deployment -n minio-operator
```


2. Create the `minio` namespace and secrets
-------------------------------------------

Create a dedicated namespace for your MinIO tenant:

```bash
kubectl create namespace minio
```

Apply the secret manifest that stores MinIO access and secret keys:

```bash
kubectl apply -f .\kuberenetes\minio\minio-secret.yaml -n minio
```

(For Linux/macOS shells, use `./kubernetes/minio/minio-secret.yaml`.)

You can verify the secret:

```bash
kubectl get secret -n minio
kubectl describe secret minio-secret -n minio
```


3. Deploy the MinIO tenant
--------------------------

Apply the tenant configuration manifest:

```bash
kubectl apply -f .\kubernetes\minio\minio-values.yaml -n minio
```

Verify that the tenant and its pods are created:

```bash
kubectl get tenant -n minio
kubectl get pods -n minio
kubectl describe tenant minio -n minio
```

Wait until all MinIO pods show `STATUS=Running`.


4. Accessing the MinIO console (optional)
-----------------------------------------

To access the MinIO console locally, port-forward the console service
(adjust service name if it differs in your manifest):

```bash
kubectl port-forward svc/minio-console -n minio 9090:9090
```

Then open `http://localhost:9090` in your browser and log in using the
credentials defined in `minio-secret.yaml`.


5. Cleanup and references
-------------------------

- Delete the MinIO tenant (keep operator):

  ```bash
  kubectl delete -f .\kubernetes\minio\minio-values.yaml -n minio
  kubectl delete -f .\kubernetes\minio\minio-secret.yaml -n minio
  kubectl delete namespace minio
  ```

- Remove the MinIO Operator:

  ```bash
  kubectl delete -k "github.com/minio/operator?ref=v7.0.1"
  kubectl delete namespace minio-operator --ignore-not-found=true
  ```

- Official docs and resources:
  - MinIO Operator GitHub: `https://github.com/minio/operator`
  - MinIO documentation: `https://min.io/docs/minio/kubernetes/upstream/index.html`
  - Concepts & best practices: `https://min.io/docs/minio/kubernetes/upstream/concepts/overview.html`