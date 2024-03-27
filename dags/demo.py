from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(dag_id='demo',
     start_date=datetime(2024, 2, 1),
     schedule_interval='@daily',
     catchup=False
     )
def sofa_dag():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    spark = SparkSubmitOperator(
        task_id='spark_processing',
        application='/home/data-engineer/Desktop/sofascore/jobs/demo.py',
        conn_id='spark_conn_id'
    )

    start >> spark >> end


sofa_dag()
