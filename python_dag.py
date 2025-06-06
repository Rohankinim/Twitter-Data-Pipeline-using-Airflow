import tweepy
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago

from twitter_etl import run_twitter_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,    
    'email': ['airflow@example.com']
}

dag = DAG(
    'twitter_etl_dag',
    default_args=default_args,
    description='A simple DAG to run Twitter ETL',
    schedule_interval='@daily',
)

run_etl = PythonOperator(
    task_id='run_twitter_etl',
    python_callable=run_twitter_etl,
    dag=dag,
)

run_etl
