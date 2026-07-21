from datetime import datetime
from airflow import DAG # pyright: ignore[reportMissingImports]
from airflow.operators.bash import BashOperator # type: ignore

with DAG(
    dag_id="APOD_pipeline",
    start_date=datetime(2026, 1, 7),
    schedule=None,
    catchup=False,
    tags=["apod"]
) as dag:
    
    apod_details_transformer = BashOperator(
        task_id = "apod_details_transformer",
        bash_command = """
        docker exec astrosight-spark \
        spark-submit /project/Spark/Transform/apod_details_transformer.py
        """,
        do_xcom_push=True
    )
    update_api_status = BashOperator(
        task_id="update_api_status",
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Configs/Spark_Core/Update_API_Status.py \
        '{{ ti.xcom_pull(task_ids="apod_details_transformer") }}'
        """
    )


    apod_details_transformer  >> update_api_status
