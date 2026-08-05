from Configs.Spark_Core import session,tables,insertion,pipeline_audit
from pyspark.sql.functions import col
import json
from datetime import datetime
import uuid

iceberg_catalog = "AstroSight"
bronze="bronze"
silver = "silver"
config = "config"


def transform_cme_data():
    spark = session.get_spark_session()
    tables.create_config_tables(spark)
    tables.create_silver_tables(spark=spark)
    successful_request_ids = []
    required_df = (spark.table(f"{iceberg_catalog}.{bronze}.api_response")
          .filter((col("API_Request_Type")=="cme")&(col("refreshed_to_silver")=="N")&(col("Response_status")==200)))

    tables_list = (
    spark.table(f"{iceberg_catalog}.{config}.api_table_mapping")
    .filter(
        (col("api_name") == "cme") &
        (col("is_active") == "Y")
    )
    .orderBy("execution_order")
    )
    
    for rec in required_df.toLocalIterator():
        raw_response = json.loads(rec['Raw_Api_Response'])
        request_id_status = True
        for tab in tables_list.toLocalIterator():
            try:
                request_id = pipeline_audit.start_audit(pipeline_stage='BRONZE_TO_SILVER',pipeline_target_table=tab.target_table,spark=spark)
                pipeline_failed=False
                payloads = []
                columns_list = (
                    spark.table(f"{iceberg_catalog}.{config}.api_column_mapping")
                    .filter(
                        (col("api_name")==tab.API_Name)&
                        (col("target_table")==tab.target_table)&
                        (col("is_active")=='Y')
                    )
                    .orderBy("column_order")
                )
                if tab.root_path=='$[*]':
                    for response in raw_response:
                        if tab.context is None:
                            payload = {}
                            for column in columns_list.toLocalIterator():
                                if column.json_source_path=='UUID':
                                    payload[column.target_column]=str(uuid.uuid4())
                                else:
                                    if column.data_type=='TIMESTAMP':
                                        payload[column.target_column]=datetime.strptime(response.get(column.json_source_path),"%Y-%m-%dT%H:%MZ")
                                    else:
                                        payload[column.target_column]=response.get(column.json_source_path)
                            if payload:
                                payloads.append(payload)
                        elif tab.context:
                            if tab.is_context_array == 'Y':
                                for resp in response[tab.context]:
                                    payload = {}
                                    for column in columns_list.toLocalIterator():
                                        if column.json_source_path=='UUID':
                                            payload[column.target_column]=str(uuid.uuid4())
                                        elif column.json_source_path.startswith("ROOT."):
                                            if column.data_type=='TIMESTAMP':
                                                field = column.json_source_path[5:]
                                                payload[column.target_column]=datetime.strptime(response.get(field),"%Y-%m-%dT%H:%MZ")
                                            else:
                                                field = column.json_source_path[5:]
                                                payload[column.target_column]=response.get(field)
                                        else:
                                            if column.data_type=='TIMESTAMP':
                                                payload[column.target_column]=datetime.strptime(resp[column.json_source_path],"%Y-%m-%dT%H:%MZ")
                                            else:
                                                payload[column.target_column]=resp[column.json_source_path]
                                    if payload:
                                        payloads.append(payload)
                                    # print(payload)
                            else:
                                resp = response[tab.context]
                                payload = {}
                                for column in columns_list.toLocalIterator():
                                    if column.json_source_path=='UUID':
                                        payload[column.target_column]=str(uuid.uuid4())
                                    else:
                                        if column.data_type=='TIMESTAMP':
                                            payload[column.target_column]=datetime.strptime(resp[column.json_source_path],"%Y-%m-%dT%H:%MZ")
                                        else:
                                            payload[column.target_column]=resp[column.json_source_path]
                                if payload:
                                    payloads.append(payload)

                if payloads:
                    merge_info = (
                        spark.table(f"{iceberg_catalog}.{config}.table_merge_mapping")
                        .filter(
                            col("table_name") == tab.target_table
                        )
                        .first()
                    )
                    merge_func = getattr(insertion, merge_info.merge_fun_name)
                    merge_func(spark=spark,payload=payloads)
            except Exception as e:
                payload = [{
                        "request_id":rec['request_id'],
                        "source_layer":"bronze",
                        "target_layer":"silver",
                        "source_table":"api_response",
                        "target_table":tab.target_table,
                        "error_message":str(e),
                        "error_timestamp":datetime.now(),
                        "status":"OPEN"
                        }]
                insertion.insert_into_PROCESSING_ERROR_LOG(payload, spark)
                request_id_status = False
                pipeline_failed=True
                print("Job Failed with ERROR",e)
            if pipeline_failed:
                pipeline_audit.end_audit(status='FAILED',request_id=request_id,spark=spark)
            else:
                pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)
        if request_id_status:
            
            # insertion.Update_api_response_status([rec["request_id"]],spark=spark)
            successful_request_ids.append(rec["request_id"])
    return successful_request_ids

if __name__ == "__main__":
    successful_request_ids = transform_cme_data()
    print(json.dumps(successful_request_ids))
