from datetime import datetime
from airflow import DAG # pyright: ignore[reportMissingImports]
from airflow.operators.bash import BashOperator # type: ignore

with DAG(
    dag_id="GST_Pipeline",
    start_date=datetime(2026, 1, 7),
    schedule=None,
    catchup=False,
    tags=["gst"]
) as dag:
    
    gst_transform = BashOperator(
        task_id="gst_transform",
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Spark/Transform/Gst_Transformer.py
        """,
        do_xcom_push=True
    )
    gst_kp_transform = BashOperator(
        task_id="gst_kp_transform",
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Spark/Transform/Gst_Kp_Transformer.py
        """,
        do_xcom_push=True
    )

    update_api_status = BashOperator(
        task_id="update_api_status",
        bash_command="""
        docker exec astrosight-spark \
        spark-submit /project/Configs/Spark_Core/Update_API_Status.py \
        '{{ ti.xcom_pull(task_ids="gst_transform") }}' \
        '{{ ti.xcom_pull(task_ids="gst_kp_transform") }}'
        """
    )

    gst_rankings_load = BashOperator(
        task_id = "gst_rankings_load",
        bash_command = """
        docker exec astrosight-spark \
        spark-submit /project/Spark/Load/DAYN/GST_Rankings_DAYN.py
        """
    )

    gst_summary_load = BashOperator(
        task_id = "gst_summary_load",
        bash_command = """
        docker exec astrosight-spark \
        spark-submit /project/Spark/Load/DAYN/GST_Summary_DAYN.py
        """
    )

    gst_distribution_load = BashOperator(
        task_id = "gst_distribution_load",
        bash_command = """
        docker exec astrosight-spark \
        spark-submit /project/Spark/Load/DAY0/GST_Distribution_DAY0.py
        """
    )

    [gst_transform,gst_kp_transform] >> update_api_status >> [gst_rankings_load,gst_summary_load,gst_distribution_load]
