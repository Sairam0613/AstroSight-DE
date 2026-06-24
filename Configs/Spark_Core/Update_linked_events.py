from Configs.Spark_Core import session,tables,insertion
from Configs.API import Resolve_Params
from pyspark.sql.functions import col,lit
import json
from datetime import datetime

iceberg_catalog = "AstroSight"
bronze_layer="bronze"
silver_layer = "silver"



def Update_linked_events():
    spark = session.get_spark_session()
    tables.create_bronze_tables(spark)

    watermark_df = spark.table(f"{iceberg_catalog}.{bronze_layer}.PIPELINE_WATERMARK").filter(col("process_name")=="linked_events").first()
    if watermark_df is not None:
        last_processed_timestamp = watermark_df["last_processed_timestamp"]
    else:
        last_processed_timestamp = None
    max_processed_timestamp = last_processed_timestamp
    max_request_id = None
    if last_processed_timestamp is not None:
        req_ids = spark.table(f"""{iceberg_catalog}.{bronze_layer}.API_RESPONSE""").filter(
            (col('refreshed_timestamp')>=lit(last_processed_timestamp)) &
            (col('refreshed_to_silver')=='Y') &
            (col('API_Request_Type').isin("gst"))

        )
    else:
        req_ids = spark.table(f"""{iceberg_catalog}.{bronze_layer}.API_RESPONSE""").filter(
            (col('refreshed_to_silver')=='Y') &
            (col('API_Request_Type').isin("gst"))
        )
    for row in req_ids.toLocalIterator():
        current_timestamp = row["refreshed_timestamp"]
        if (max_processed_timestamp is None or max_processed_timestamp<current_timestamp):
            max_processed_timestamp=current_timestamp
            max_request_id = row['request_id']
        try:
            raw_response = json.loads(row['Raw_Api_Response'])

            req_data = Resolve_Params.resolve_linked_params(row['API_Request_Type'])
            for raw in raw_response:
                for j in raw.get("linkedEvents", []):
                    payload = [{
                        "source_id":raw[req_data['source_pk']],
                        "source_type":raw[req_data['source_pk']].split('-')[-2],
                        "activity_id":j.get(req_data["target_pk"]),
                        "activity_type":j.get(req_data["target_pk"]).split('-')[-2] if j.get(req_data["target_pk"]) else None,
                        "ingestion_timestamp":datetime.now()
                    }]
                    insertion.insert_into_linked_events(payload=payload,spark=spark)
        
        except Exception as e:
            payload = [{
                "request_id":row['request_id'],
                "source_layer":"bronze",
                "target_layer":"silver",
                "source_table":"api_response",
                "target_table":"linked_events",
                "error_message":str(e),
                "error_timestamp":datetime.now(),
                "status":"OPEN"
            }]
            insertion.insert_into_PROCESSING_ERROR_LOG(payload, spark)
            print("job failed with error",e)
    if max_processed_timestamp is not None:
        payload_watermark = [{
            "process_name": "linked_events",
            "last_processed_request_id": max_request_id,
            "last_processed_timestamp": max_processed_timestamp,
            "updated_timestamp": datetime.now()
        }]
        insertion.update_pipeline_watermark(payload_watermark,spark)
