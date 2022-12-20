from datetime import datetime, timedelta
from scripts import func
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'Chris',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_artist_revenue_table',
    default_args=default_args,
    start_date=datetime(2022, 12, 18),
    schedule_interval= '0 7 * * *'
) as dag:
    
    start = DummyOperator(
        task_id="Start"
    )
     
    create_table_artist_revenue_task = PythonOperator(
        task_id="create_table_artist_revenue",
        python_callable=func.denormalize_artist_revenue
    )
    
    end = DummyOperator(
        task_id="End"
    )
    
    start >> create_table_artist_revenue_task >> end