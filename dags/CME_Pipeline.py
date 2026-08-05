from datetime import datetime
from airflow import DAG # pyright: ignore[reportMissingImports]
from airflow.operators.bash import BashOperator # type: ignore

with DAG(
    dag_id="CME_pipeline",
    start_date=datetime(2026, 1, 7),
    schedule=None,
    catchup=False,
    tags=["cme"]
) as dag:
    
    cme_transformer = BashOperator(
        task_id = "cme_transformer",
        bash_command = """
        docker exec astrosight-spark \
        spark-submit /project/Spark/Transform/CME_Transformer.py
        """,
        do_xcom_push=True
    )
    update_api_status = BashOperator(
        task_id="update_api_status",
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Configs/Spark_Core/Update_API_Status.py \
        '{{ ti.xcom_pull(task_ids="cme_transformer") }}'
        """
    )

    cme_activity_score_load = BashOperator(
        task_id='cme_activity_score_load',
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Spark/Load/DAYN/CME_Activity_Score_DAYN.py
        """
    )

    cme_summary_load = BashOperator(
        task_id='cme_summary_load',
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Spark/Load/DAYN/CME_SUMMARY_DAYN.py
        """
    )

    cme_transformer  >> update_api_status >> cme_activity_score_load >> cme_summary_load
