from airflow import DAG
from datetime import datetime
from email.mime import application
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from random import randint


with DAG(
    dag_id='holobalance_sitting_dag',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['homepage']                                            # IMPORTANT: Specify the tag 'analytics' in the analytics that the clinicians should be see in the UI that triggers manually the execution
) as dag:

    # IMPORTANT: example of how to specify a Spark Jobs that can read from Delta Tables. Packages, conf, conn_id, they should contains always those kind of configuration. 
    # You can add more packages if you need it in the task (they will be downloaded automatically) 
    updater = SparkSubmitOperator(
        application="hdfs://namenode:9000/scripts/holobalance_sitting.py",						# We have a folder in the HDFS file system called scripts so we can upload files there
        task_id="updater",
        packages="io.delta:delta-core_2.12:1.1.0,org.postgresql:postgresql:42.3.3",
        conf={
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension', 
            'spark.sql.catalog.spark_catalog':'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            'spark.sql.warehouse.dir':'hdfs://namenode:9000/user/spark/db'
        }, 
        conn_id='spark_default'
    )

