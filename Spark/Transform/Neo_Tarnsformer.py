from Configs.Spark_Core import session,tables,insertion,pipeline_audit
from pyspark.sql.functions import col
import json
from datetime import datetime

iceberg_catalog = "AstroSight"
bronze="bronze"
silver = "silver"


def transform_neo_data():
    spark = session.get_spark_session()
    request_id = pipeline_audit.start_audit(pipeline_stage='BRONZE_TO_SILVER',pipeline_target_table='neo_objects',spark=spark)
    pipeline_failed=False
    session.create_namespaces(spark)
    tables.create_silver_tables(spark)
    successful_request_ids = []
    required_df = (spark.table(f"{iceberg_catalog}.{bronze}.api_response")
          .filter((col("API_Request_Type")=="neo")&(col("refreshed_to_silver")=="N")&(col("Response_status")==200)))
    for rec in required_df.toLocalIterator():
        try:
            raw_response = json.loads(rec['Raw_Api_Response'])
            for feed_date,asteroids in raw_response['near_earth_objects'].items():
                for row in asteroids:
                    payload = [{
                        'Asteroid_Id': row['id'],
                        'Asteroid_Name': row['name'],
                        'absolute_magnitude': row['absolute_magnitude_h'],
                        'estimated_diameter_min_kms': row['estimated_diameter']['kilometers']['estimated_diameter_min'],
                        'estimated_diameter_max_kms': row['estimated_diameter']['kilometers']['estimated_diameter_max'],
                        'is_potentially_hazardous': row['is_potentially_hazardous_asteroid'],
                        'nasa_jpl_url': row['nasa_jpl_url'],
                        'feed_date':datetime.strptime(feed_date,"%Y-%m-%d").date(),
                        'ingestion_timestamp': datetime.now()
                    }]
                    insertion.merge_into_neo_objects(payload, spark)
            successful_request_ids.append(rec["request_id"])
        except Exception as e:
            payload = [{
                "request_id":rec['request_id'],
                "source_layer":"bronze",
                "target_layer":"silver",
                "source_table":"api_response",
                "target_table":"neo_objects",
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
    successful_request_ids = transform_neo_data()
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
        # request_ids = dbutils.jobs.taskValues.get(
        # taskKey="Test_Run",
        # key="successful_request_ids",
        # debugValue=[]
        # )
        # print("Retrieved Task Value:")
        # print(request_ids)
        
    