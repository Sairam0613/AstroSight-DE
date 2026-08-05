from Configs.Spark_Core import session,tables,insertion,pipeline_audit
from pyspark.sql.functions import col
import json
from datetime import datetime

iceberg_catalog = "AstroSight"
bronze_layer="bronze"
silver_layer = "silver"

def gst_kp_details():
    spark = session.get_spark_session()
    request_id = pipeline_audit.start_audit(pipeline_stage='BRONZE_TO_SILVER',pipeline_target_table='gst_kp_details',spark=spark)
    pipeline_failed=False
    session.create_namespaces(spark)
    tables.create_silver_tables(spark)
    successful_request_ids = []
    required_df = (spark.table(f"{iceberg_catalog}.{bronze_layer}.api_response")
          .filter((col("API_Request_Type")=="gst")&(col("refreshed_to_silver")=="N")&(col("Response_status")==200)))
    for rec in required_df.toLocalIterator():
        try:
            raw_response = json.loads(rec['Raw_Api_Response'])
            for raw in raw_response:
                for kp in raw['allKpIndex']:
                    payload = [{
                        "gst_id":raw['gstID'],
                        "kp_observed_time":datetime.strptime(kp['observedTime'],"%Y-%m-%dT%H:%MZ"),
                        "kp_index":kp['kpIndex'],
                        "kp_source":kp['source'],
                        "ingestion_timestamp":datetime.now()
                    }]
                    insertion.insert_into_gst_kp_details(payload,spark)
                successful_request_ids.append(rec["request_id"])
            # pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)
        except Exception as e:
            payload = [{
                "request_id":rec['request_id'],
                "source_layer":"bronze",
                "target_layer":"silver",
                "source_table":"api_response",
                "target_table":"gst_kp_details",
                "error_message":str(e),
                "error_timestamp":datetime.now(),
                "status":"OPEN"
            }]
            insertion.insert_into_PROCESSING_ERROR_LOG(payload, spark)
            pipeline_failed=True
            # pipeline_audit.end_audit(status='FAILED',request_id=request_id,spark=spark)
            print("job failed with error",e)
    if pipeline_failed:
        pipeline_audit.end_audit(status='FAILED',request_id=request_id,spark=spark)
    else:
        pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)
    return successful_request_ids

if __name__ == "__main__":
    successful_request_ids = gst_kp_details()
    print(json.dumps(successful_request_ids))