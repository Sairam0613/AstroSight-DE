from Configs.Spark_Core import session,tables,insertion
from pyspark.sql.functions import col
import json
from datetime import datetime

iceberg_catalog = "AstroSight"
bronze_layer="bronze"
silver_layer = "silver"

def transform_neo_close_data():
    spark = session.get_spark_session()
    session.create_namespaces(spark)
    tables.create_silver_tables(spark)
    successful_request_ids = []
    required_df = (spark.table(f"{iceberg_catalog}.{bronze_layer}.api_response")
          .filter((col("API_Request_Type")=="neo")&(col("refreshed_to_silver")=="N")&(col("Response_status")==200)))
    for rec in required_df.toLocalIterator():
        try:
            raw_response = json.loads(rec['Raw_Api_Response'])
            for row in raw_response['near_earth_objects'][list(raw_response['near_earth_objects'].keys())[0]]:
                payload = [{
                    "Asteroid_Id": row["id"],
                    "close_approach_date_full": datetime.strptime(row["close_approach_data"][0]["close_approach_date_full"],"%Y-%b-%d %H:%M"),
                    "miss_distance_kms": float(row["close_approach_data"][0]["miss_distance"]["kilometers"]),
                    "miss_distance_miles": float(row["close_approach_data"][0]["miss_distance"]["miles"]),
                    "miss_distance_lunar": float(row["close_approach_data"][0]["miss_distance"]["lunar"]),
                    "relative_velocity_kmph": float(
                        row["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"]),
                    "relative_velocity_kmps": float(
                        row["close_approach_data"][0]["relative_velocity"]["kilometers_per_second"]),
                    "orbiting_body": row["close_approach_data"][0]["orbiting_body"],
                    "ingestion_timestamp": datetime.now()
                }]
                insertion.insert_into_neo_close_approach(payload, spark)
            successful_request_ids.append(rec["request_id"])
        except Exception as e:
            payload = [{
                "request_id":rec['request_id'],
                "source_layer":"bronze",
                "target_layer":"silver",
                "source_table":"api_response",
                "target_table":"neo_close_approaches",
                "error_message":str(e),
                "error_timestamp":datetime.now(),
                "status":"OPEN"
            }]
            insertion.insert_into_PROCESSING_ERROR_LOG(payload, spark)
    return successful_request_ids

if __name__ == "__main__":
    successful_request_ids = transform_neo_close_data()
    print(json.dumps(successful_request_ids))