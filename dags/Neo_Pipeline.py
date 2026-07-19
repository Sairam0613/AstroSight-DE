from datetime import datetime
from airflow import DAG # pyright: ignore[reportMissingImports]
from airflow.operators.bash import BashOperator # pyright: ignore[reportMissingImports]

with DAG(
    dag_id="Neo_Pipeline",
    start_date=datetime(2026, 1, 7),
    schedule=None,
    catchup=False,
    tags=["neo"]
) as dag:

    neo_extract = BashOperator(
        task_id="neo_extract",
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Spark/Extract/NeoWS_Extract.py
        """
    )

    neo_tranform = BashOperator(
        task_id="neo_transform",
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Spark/Transform/Neo_Tarnsformer.py
        """,
        do_xcom_push=True
    )

    neo_approach_transform = BashOperator(
        task_id="neo_approach_transform",
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Spark/Transform/Neo_Approach_Transformer.py
        """,
        do_xcom_push=True
    )

    update_api_status = BashOperator(
        task_id="update_api_status",
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Configs/Spark_Core/Update_API_Status.py \
        '{{ ti.xcom_pull(task_ids="neo_transform") }}' \
        '{{ ti.xcom_pull(task_ids="neo_approach_transform") }}'
        """
    )

    neo_rankings_load = BashOperator(
        task_id = "neo_rankings_load",
        bash_command = """
        docker exec astrosight-spark \
        spark-submit /project/Spark/Load/DAYN/Neo_Rankings_Load_DAYN.py
        """
    )

    neo_summary_load = BashOperator(
        task_id = "neo_summary_load",
        bash_command = """
        docker exec astrosight-spark \
        spark-submit /project/Spark/Load/DAYN/Neo_Summary_Load_DAYN.py
        """
    )

    neo_extract >> neo_tranform >> neo_approach_transform >> update_api_status >> neo_rankings_load >> neo_summary_load