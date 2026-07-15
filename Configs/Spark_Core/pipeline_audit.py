import uuid
from pyspark.sql.functions import col,lit
import json
from datetime import datetime,date
from Configs.Spark_Core import insertion

iceberg_catalog = "AstroSight"
bronze_layer="bronze"
silver_layer = "silver"

def start_audit(pipeline_stage,pipeline_target_table,spark):
    try:
        request_id= str(uuid.uuid4())
        payload = [{
                        "Pipeline_Audit_ID":request_id,
                        "Pipeline_run_date":date.today(),
                        "pipeline_stage":pipeline_stage,
                        "pipeline_target_table":pipeline_target_table,
                        "pipeline_start_time": datetime.now(),
                        "pipeline_end_time":None,
                        "pipeline_stage_status":'STARTED',
                        "pipeline_expected_records":None,
                        "pipeline_processed_records":None,
                        "ingestion_timestamp":datetime.now()
                    }]
        insertion.insert_into_pipeline_audit(spark,payload)
        return request_id            
    except Exception as e:
        print("Failed with Error",e)
        raise

def end_audit(status,request_id,spark):
    try:
        insertion.Update_pipeline_audit(status,request_id,spark)
    except Exception as e:
        print("Failed with Error",e)
        raise