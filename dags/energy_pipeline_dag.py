from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/scripts')

from fetch_energy_data import fetch_energy_data, save_to_postgres

default_args = {
    'owner': 'samereh',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'italian_energy_pipeline',
    default_args=default_args,
    description='Fetches Italian electricity demand data daily',
    schedule_interval='@daily',
    catchup=False,
)

def run_fetch():
    df = fetch_energy_data()
    save_to_postgres(df)

fetch_task = PythonOperator(
    task_id='fetch_and_store_energy_data',
    python_callable=run_fetch,
    dag=dag,
)