from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/src')

from job3_analytics import compute_daily_analytics

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'job3_crypto_daily_analytics',
    default_args=default_args,
    description='DAG 3: Daily analytics job - compute cryptocurrency summary from SQLite',
    schedule_interval='@daily',
    catchup=False,
    tags=['analytics', 'sqlite', 'summary', 'cryptocurrency']
)

analytics_task = PythonOperator(
    task_id='compute_crypto_daily_summary',
    python_callable=compute_daily_analytics,
    dag=dag
)