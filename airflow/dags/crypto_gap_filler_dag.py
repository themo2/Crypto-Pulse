from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'cryptopulse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crypto_gap_filler_dag',
    default_args=default_args,
    description='update the  Binance data in the database to fill the gaps',
    schedule_interval='*/10 * * * *', 
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=['cryptopulse', 'processing'],
) as dag:

    run_gap_filler = BashOperator(
        task_id='run_gap_filler_script',
        # Edit the path to the Python script within the Airflow container
        bash_command='python /opt/airflow/scripts/update_db.py ',
    )