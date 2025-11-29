helm repo add apache-airflow https://airflow.apache.org
helm repo update

kubectl create namespace airflow
kubectl apply -f ./kubernetes/airflow/airflow-secret.yaml