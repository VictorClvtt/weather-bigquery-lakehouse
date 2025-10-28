from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from include.src.etl.bronze_ingest import bronze_ingest

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="bigquery_weather_dag",
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    tags=["bigquery", "spark", "weather", "lakehouse"],
    description="Ingestão e transformação de dados meteorológicos até a camada Gold no BigQuery.",
)
def bigquery_weather_dag():
    bronze_ingest_task = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_ingest,
    )

    bronze_to_silver_task = SparkSubmitOperator(
        task_id="bronze_to_silver",
        conn_id="my_spark_conn",
        application="include/src/etl/bronze_to_silver.py",
        verbose=True,
    )

    silver_to_gold_task = SparkSubmitOperator(
        task_id="silver_to_gold",
        conn_id="my_spark_conn",
        application="include/src/etl/silver_to_gold.py",
        verbose=True,
    )

    bronze_ingest_task >> bronze_to_silver_task >> silver_to_gold_task


bigquery_weather_dag()
