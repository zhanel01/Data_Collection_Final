from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/src')

from job1_producer import produce_to_kafka

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'job1_crypto_continuous_ingestion',
    default_args=default_args,
    description='DAG 1: Continuous cryptocurrency data ingestion from CoinGecko API to Kafka',
    schedule_interval= None,
    catchup=False,
    tags=['ingestion', 'kafka', 'cryptocurrency', 'coingecko']
)

def run_producer():
    produce_to_kafka()

ingestion_task = PythonOperator(
    task_id='fetch_crypto_and_produce_to_kafka',
    python_callable=run_producer,
    dag=dag
)