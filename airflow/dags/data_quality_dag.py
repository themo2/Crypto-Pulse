from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

default_args = {
    'owner': 'cryptopulse',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='crypto_data_quality_dag',
    default_args=default_args,
    description='Daily PostgreSQL integrity and completeness check',
    schedule='0 2 * * *',  
    catchup=False,
) as dag:

    check_no_nulls = SQLCheckOperator(
        task_id='verify_no_null_prices',
        conn_id='postgres_cryptopulse',
        sql="""
            SELECT COUNT(*) = 0 
            FROM historical_prices_1m 
            WHERE close IS NULL OR volume IS NULL;
        """
    )

    check_no_nulls