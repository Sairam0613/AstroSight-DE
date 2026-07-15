from Configs.Spark_Core import session,insertion,pipeline_audit
from Configs.API import Resolve_Params,API_HIT
from pyspark.sql.functions import col
iceberg_catalog = "AstroSight"
bronze="bronze"


def execute_scheduled_requests():
    spark = session.get_spark_session()
    request_id = pipeline_audit.start_audit(pipeline_stage='API_TO_BRONZE',pipeline_target_table='api_response',spark=spark)
    df = spark.table(f"{iceberg_catalog}.{bronze}.api_endpoints")\
          .filter(col("endpoint_type")=="scheduled")\
          .filter(col("is_active")=="Y")\
          .filter(col("api_name")=="NASA")
    for row in df.collect():
        try:
            data = Resolve_Params.resolve_params(row['request_params'])
            endpoint_url = row['endpoint_url']
            data,status = API_HIT.get_url_response(endpoint_url,data)
            if status == 200:
                payload = {
                    "URL_Endpoint": endpoint_url,
                    "API_Request_Type": row['endpoint_name'],
                    "Entity_Requested": row['endpoint_name'],
                    "Raw_Api_Response": data,
                    "Response_status": status,
                    "error_msg": None
                }
            else:
                payload = {
                    "URL_Endpoint": endpoint_url,
                    "API_Request_Type": row['endpoint_name'],
                    "Entity_Requested": row['endpoint_name'],
                    "Raw_Api_Response": None,
                    "Response_status": status,
                    "error_msg": data
                }
            insertion.insert_into_api_response(payload,spark)
            pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)
        except Exception as e:
            print(f"Job Failed with error {e}")
            pipeline_audit.end_audit(status='FAILED',request_id=request_id,spark=spark)

if __name__ == "__main__":
    execute_scheduled_requests()