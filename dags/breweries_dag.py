from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import settings
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection

conn = Connection(
    conn_id='spark_standalone',
    conn_type='spark',
    host='spark://spark-master:7077',
    description='Conexão local com Spark Standalone',
    extra='{"deploy-mode": "client"}'
)

try:
    BaseHook.get_connection(conn.conn_id)
except AirflowNotFoundException:
    session = settings.Session()
    session.add(conn)
    session.commit()
    print(f"Conexão {conn.conn_id} criada com sucesso!")


with DAG(dag_id="Breweries", start_date=datetime(2025, 3, 31), schedule="0 0 31 * *") as dag:
    breweries_bronze = SparkSubmitOperator(
        task_id="breweries_bronze",
        conn_id="spark_standalone",
        application="./src/breweries_bronze.py"
    )
    
    breweries_silver = SparkSubmitOperator(
        task_id="breweries_silver",
        conn_id="spark_standalone",
        application="./src/breweries_silver.py"
    )
    
    breweries_by_type_and_location = SparkSubmitOperator(
        task_id="breweries_by_type_and_location",
        conn_id="spark_standalone",
        application="./src/breweries_by_type_and_location.py"
    )    
    
    breweries_bronze >> breweries_silver >> breweries_by_type_and_location        
