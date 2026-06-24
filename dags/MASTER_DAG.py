from airflow import DAG # pyright: ignore[reportMissingImports]
from airflow.operators.empty import EmptyOperator # pyright: ignore[reportMissingImports]
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # pyright: ignore[reportMissingImports]
from datetime import datetime, timedelta

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
    schedule_interval="30 10 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["AstroSight", "Master"]
) as dag:

    start = EmptyOperator(
        task_id="start"
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


    end = EmptyOperator(
        task_id="end"
    )

    start >>  neo_pipeline >> gst_pipeline >> end