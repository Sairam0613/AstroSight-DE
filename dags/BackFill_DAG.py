from datetime import datetime
from airflow import DAG # pyright: ignore[reportMissingImports]
from airflow.operators.bash import BashOperator # type: ignore

with DAG(
    dag_id="BACKFILL_pipeline",
    start_date=datetime(2026, 1, 7),
    schedule=None,
    catchup=False,
    tags=["Backfill"]
) as dag:
    
    backfill_dag = BashOperator(
        task_id = "backfill_extraction",
        bash_command = """
        docker exec astrosight-spark \
        spark-submit /project/Spark/Extract/API_BackFill_Extract.py
        """
                )
    update_backfill_control = BashOperator(
        task_id="update_backfill_control",
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Spark/Control/update_backfill_control.py 
        """
    )


    backfill_dag  >> update_backfill_control
