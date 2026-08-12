from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="master_crypto_orchestrator",
    description="Master orchestrator that executes all pipeline steps in order",
    start_date=datetime(2026, 8, 1),
    schedule=None,  
    catchup=False,
    tags=["master", "cryptopulse", "orchestration"]
) as dag:

    
    trigger_gap_filler = TriggerDagRunOperator(
        task_id="trigger_gap_filler",
        trigger_dag_id="crypto_gap_filler_dag",
        wait_for_completion=True,
        poke_interval=30,  
    )

    
    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="crypto_data_quality_dag",
        wait_for_completion=True,
        poke_interval=30,
    )

    
    trigger_gbt_retrain = TriggerDagRunOperator(
        task_id="trigger_gbt_retrain",
        trigger_dag_id="crypto_gbt_retrain_dag",
        wait_for_completion=False,  
    )

    trigger_gap_filler >> trigger_data_quality >> trigger_gbt_retrain