from airflow import DAG # pyright: ignore[reportMissingImports]
from airflow.operators.empty import EmptyOperator # pyright: ignore[reportMissingImports]
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # pyright: ignore[reportMissingImports]
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator # type: ignore

default_args = {
    "owner": "sairam",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10)
}

with DAG(
    dag_id="ASTROSIGHT_MASTER_DAG",
    default_args=default_args,
    description="Master DAG for AstroSight",
    start_date=datetime(2026, 1, 1),
    #schedule_interval="30 8,23 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["AstroSight", "Master"]
) as dag:

    start = EmptyOperator(
            task_id="start"
        )

    backfill_dag = BashOperator(
            task_id = "backfill_extraction",
            bash_command = """
            docker exec astrosight-spark \
            spark-submit /project/Spark/Extract/API_BackFill_Extract.py
            """
                    )
    neo_pipeline = TriggerDagRunOperator(
            task_id="trigger_neo_pipeline",
            trigger_dag_id="Neo_Pipeline",   # Replace with actual NEO DAG ID
            wait_for_completion=True,
            poke_interval=30,
            reset_dag_run=True,
            allowed_states=["success"],
            failed_states=["failed"]
        )
    
    gst_pipeline = TriggerDagRunOperator(
        task_id="trigger_gst_pipeline",
        trigger_dag_id="GST_Pipeline",   # Replace with actual GST DAG ID
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    apod_pipeline = TriggerDagRunOperator(
        task_id = "trigger_apod_pipeline",
        trigger_dag_id = "APOD_pipeline",
        wait_for_completion=True,
        poke_interval = 30,
        reset_dag_run = True,
        allowed_states = ['success'],
        failed_states = ['failed']
    )

    cme_pipeline = TriggerDagRunOperator(
            task_id = "trigger_cme_pipeline",
            trigger_dag_id = "CME_pipeline",
            wait_for_completion=True,
            poke_interval = 30,
            reset_dag_run = True,
            allowed_states = ['success'],
            failed_states = ['failed']
        )

    IPS_pipeline = TriggerDagRunOperator(
                task_id = "trigger_IPS_pipeline",
                trigger_dag_id = "IPS_pipeline",
                wait_for_completion=True,
                poke_interval = 30,
                reset_dag_run = True,
                allowed_states = ['success'],
                failed_states = ['failed']
            )
    update_backfill_control = BashOperator(
            task_id="update_backfill_control",
            bash_command="""
            docker exec astrosight-spark \
            spark-submit /project/Spark/Control/update_backfill_control.py 
            """
        )

    end = EmptyOperator(
            task_id="end"
        )

    start >> backfill_dag >> neo_pipeline >> gst_pipeline >> apod_pipeline >> cme_pipeline >> IPS_pipeline  >> update_backfill_control >> end