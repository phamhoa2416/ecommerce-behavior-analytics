from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    'daily_kafka_to_hdfs',
    start_date=datetime(2019, 11, 1),
    schedule_interval='20 9 * * *',
    catchup=False,
    tags=['spark', 'kafka', 'hdfs']
) as dag:

    submit = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/opt/spark_jobs/spark_kafka_to_hdfs.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'cluster'
        },
        application_args=[],
        env_vars={
            'HADOOP_CONF_DIR': '/opt/hadoop/etc/hadoop',
            'SPARK_HOME': '/opt/spark'
        }
    )