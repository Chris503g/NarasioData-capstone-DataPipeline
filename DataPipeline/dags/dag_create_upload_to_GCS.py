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
    dag_id='dag_create_upload_to_GCS',
    default_args=default_args,
    start_date=datetime(2022, 12, 18),
    schedule_interval= '0 9 * * *'
) as dag:
    
    start = DummyOperator(
        task_id="Start"
    )
     
    create_bq_one_file = PythonOperator(
        task_id="create_bq_one",
        python_callable=func.create_bq_one
    )
    
    create_bq_two_file = PythonOperator(
        task_id="create_bq_two",
        python_callable=func.create_bq_two
    )
    
    create_bq_third_file = PythonOperator(
        task_id="create_bq_third",
        python_callable=func.create_bq_third
    )
    
    create_bq_fourth_file = PythonOperator(
        task_id="create_bq_fourth",
        python_callable=func.create_bq_fourth
    )
    
    upload_file_to_GCS = PythonOperator(
        task_id="upload_file_to_GCS",
        python_callable=func.upload_file_to_GCS
    )
    
    end = DummyOperator(
        task_id="End"
    )
    
    start >> create_bq_one_file >> create_bq_two_file >> create_bq_third_file >>create_bq_fourth_file >> upload_file_to_GCS >> end