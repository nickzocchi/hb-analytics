from airflow import DAG
from datetime import datetime
from email.mime import application
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from random import randint


with DAG(
    dag_id='holobalance_score_dag',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['homepage']                                            
) as dag:

    updater = SparkSubmitOperator(
        application="hdfs://namenode:9000/scripts/holobalance_score.py",						
        task_id="updater",
        packages="io.delta:delta-core_2.12:1.1.0,org.postgresql:postgresql:42.3.3",
        conf={
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension', 
            'spark.sql.catalog.spark_catalog':'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            'spark.sql.warehouse.dir':'hdfs://namenode:9000/user/spark/db'
        }, 
        conn_id='spark_default'
    )




