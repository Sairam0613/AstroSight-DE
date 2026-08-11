from Configs.Spark_Core import session,tables,insertion,pipeline_audit
from pyspark.sql.functions import col
import json
from datetime import datetime

iceberg_catalog = "AstroSight"
bronze="bronze"
silver = "silver"


def apod_details_transform():
    spark = session.get_spark_session()
    request_id = pipeline_audit.start_audit(pipeline_stage='BRONZE_TO_SILVER',pipeline_target_table='apod_details',spark=spark)
    pipeline_failed=False
    tables.create_silver_tables(spark)
    successful_request_ids = []
    required_df = (spark.table(f"{iceberg_catalog}.{bronze}.api_response")
          .filter((col("API_Request_Type")=="apod")&(col("refreshed_to_silver")=="N")&(col("Response_status")==200)))
    for rec in required_df.toLocalIterator():
        try:
            raw_response = json.loads(rec['Raw_Api_Response'])
            for row in raw_response:
                payload=[{
                  'feed_date':datetime.strptime(row['date'],"%Y-%m-%d").date(),
                  'apod_title':row['title'],
                  'apod_media_type':row['media_type'],
                  'apod_explanation':row['explanation'],
                  'apod_url':row['url'],
                  'apod_hd_url':row['hdurl'],
                  'apod_copyright':row.get('copyright'),
                  'apod_service_version':row['service_version'],
                  'ingestion_timestamp':datetime.now()  
                }]
                insertion.merge_into_apod_details(spark=spark,payload=payload)
            successful_request_ids.append(rec["request_id"])
            # pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)
        except Exception as e:
            payload = [{
                "request_id":rec['request_id'],
                "source_layer":"bronze",
                "target_layer":"silver",
                "source_table":"api_response",
                "target_table":"apod_details",
                "error_message":str(e),
                "error_timestamp":datetime.now(),
                "status":"OPEN"
            }]
            insertion.insert_into_PROCESSING_ERROR_LOG(payload, spark)
            pipeline_failed=True
            # pipeline_audit.end_audit(status='FAILED',request_id=request_id,spark=spark)
    if pipeline_failed:
        pipeline_audit.end_audit(status='FAILED',request_id=request_id,spark=spark)
    else:
        pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)
    return successful_request_ids

if __name__ == "__main__":
    successful_request_ids = apod_details_transform()
    print(json.dumps(successful_request_ids))
    import sys
    if 'PLATFORM_TYPE' in sys.argv:
        platform_type = sys.argv[sys.argv.index('PLATFORM_TYPE')+1]
        print("Setting Task Value:",successful_request_ids)
        if platform_type=='DATABRICKS':
            dbutils.jobs.taskValues.set(
                key="successful_request_ids",
                value = successful_request_ids
            )