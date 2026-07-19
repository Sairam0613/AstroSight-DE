import json
import uuid
from datetime import datetime
from Configs.Spark_Core import Schemas,Schema_Validation

iceberg_catalog = "AstroSight"
bronze="bronze"
silver_layer="silver"
gold_layer="gold"


def insert_into_api_response(payload,spark):
    data = [{
        "request_id":str(uuid.uuid4()),
        "URL_Endpoint": payload["URL_Endpoint"],
        "API_Request_Type": payload["API_Request_Type"],
        "Request_Params": payload["URL_Endpoint"].split("?")[1] if "?" in payload["URL_Endpoint"] else None,
        "Entity_Requested": payload["Entity_Requested"],
        "Raw_Api_Response": json.dumps(payload["Raw_Api_Response"]),
        "Response_status": payload["Response_status"],
        "refreshed_to_silver": "N",
        "refreshed_timestamp": None,
        "error_msg": payload["error_msg"],
        "ingestion_timestamp": datetime.now()
    }]
    schema = Schemas.Bronze_api_response_schema()
    df = spark.createDataFrame(data,schema=schema)
    print("Inserting API response into Bronze layer")
    df.writeTo(f"{iceberg_catalog}.{bronze}.api_response").append()

def merge_into_neo_objects(payload,spark):
    try:
        schema = Schemas.Neo_Objects_Schema()
        df = spark.createDataFrame(payload,schema=schema)
        df.createOrReplaceTempView("new_data")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{silver_layer}.neo_objects as target
            USING new_data AS source
            ON target.Asteroid_Id=source.Asteroid_Id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    except Exception as e:
        print("Merge Failed with error:",e)

def insert_into_neo_close_approach(payload,spark):
    try:
        schema = Schemas.Neo_close_approach_Schema()
        payload[0]['Approach_id'] = str(uuid.uuid4())
        df = spark.createDataFrame(payload, schema=schema)
        df.createOrReplaceTempView("neo_approach_data")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{silver_layer}.neo_close_approaches as target
            USING neo_approach_data AS source
            ON target.Asteroid_Id=source.Asteroid_Id 
            and target.close_approach_date_full=source.close_approach_date_full
            when matched then UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    except Exception as e:
        print("Merge Failed with Error:",e)

def insert_into_PROCESSING_ERROR_LOG(payload,spark):
    schema = Schemas.PROCESSING_ERROR_LOG_Schema()
    payload[0]['error_id']=str(uuid.uuid4())
    df = spark.createDataFrame(payload, schema=schema)
    df.writeTo(f"{iceberg_catalog}.{bronze}.processing_error_log").append()

def Update_api_response_status(request_id,spark):
    print(f"Updating API response status to Y for request ids : {request_id}")
    ids = ",".join([f"'{id}'" for id in request_id])
    spark.sql(f"""
        UPDATE {iceberg_catalog}.{bronze}.api_response
        SET refreshed_to_silver = 'X',
        refreshed_timestamp = current_timestamp()
        WHERE request_id in ({ids})
    """)

def Mark_api_response_as_failed(request_id,spark):
    print(f"Updating API response status to P for request ids : {request_id}")
    ids = ",".join([f"'{id}'" for id in request_id])
    spark.sql(f"""
        UPDATE {iceberg_catalog}.{bronze}.api_response
        SET refreshed_to_silver = 'P',
        refreshed_timestamp = current_timestamp()
        WHERE request_id in ({ids})
    """)

def insert_into_neo_summary(df,spark):
    schema = Schemas.Neo_Summary_Schema()
    df = Schema_Validation.validate_and_cast_schema(df, schema)
    if df is not None:
        df.createOrReplaceTempView("neo_summary_data")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{gold_layer}.neo_summary as target
            USING neo_summary_data AS source
            ON target.summary_date = source.summary_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

def insert_into_neo_rankings(df,spark):
    schema = Schemas.Neo_Rankings_Schema()
    df = Schema_Validation.validate_and_cast_schema(df, schema)
    if df is not None:
        df.writeTo(f"{iceberg_catalog}.{gold_layer}.neo_rankings").createOrReplace()
    else:
        print("DataFrame is Not Available for Insertion into neo_rankings table")

def insert_into_gst_ids(payload, spark):
    schema = Schemas.GST_IDs_Schema()
    df = spark.createDataFrame(payload, schema=schema)
    df.createOrReplaceTempView("gst_ids_data")
    spark.sql(f"""
        MERGE INTO {iceberg_catalog}.{silver_layer}.gst_ids tgt
        USING gst_ids_data src
        ON src.gst_id = tgt.gst_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def insert_into_gst_kp_details(payload,spark):
    payload[0]['gst_kp_id']=str(uuid.uuid4())
    schema = Schemas.GST_KP_DETAILS_Schema()
    df = spark.createDataFrame(payload,schema)
    df.createOrReplaceTempView("gst_kp_data")
    spark.sql(f"""
        MERGE INTO {iceberg_catalog}.{silver_layer}.gst_kp_details TGT
        USING gst_kp_data SRC
        ON SRC.gst_id=TGT.gst_id AND SRC.kp_observed_time=TGT.kp_observed_time
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def insert_into_linked_events(payload,spark):
    payload[0]['linked_event_id']=str(uuid.uuid4())
    schema = Schemas.Linked_Events_Schema()
    df = spark.createDataFrame(payload,schema)
    df.createOrReplaceTempView("linked_events_data")
    spark.sql(f"""
        MERGE INTO {iceberg_catalog}.{silver_layer}.linked_events TGT
        USING linked_events_data SRC
        ON SRC.source_id=TGT.source_id AND SRC.activity_id=TGT.activity_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def update_pipeline_watermark(payload,spark):
    schema = Schemas.PIPELINE_WATERMARK_Schema()
    print(payload)
    df = spark.createDataFrame(payload,schema)
    df.createOrReplaceTempView("pipeline_data")
    spark.sql(f"""
        MERGE INTO {iceberg_catalog}.{bronze}.pipeline_watermark TGT
        USING pipeline_data SRC
        ON SRC.process_name=TGT.process_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def insert_into_gst_summary(df,spark):
    schema = Schemas.GST_Summary_Schema()
    df = Schema_Validation.validate_and_cast_schema(df, schema)
    if df is not None:
        df.createOrReplaceTempView("gst_summary_data")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{gold_layer}.gst_summary as target
            USING gst_summary_data AS source
            ON target.summary_date = source.summary_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

def insert_into_gst_rankings(df,spark):
    schema = Schemas.GST_Rankings_Schema()
    df = Schema_Validation.validate_and_cast_schema(df, schema)
    if df is not None:
        df.writeTo(f"{iceberg_catalog}.{gold_layer}.gst_rankings").createOrReplace()
    else:
        print("DataFrame is Not Available for Insertion into neo_rankings table")

def insert_into_gst_distribution(df,spark):
    schema = Schemas.GST_Distribution_Schema()
    df = Schema_Validation.validate_and_cast_schema(df, schema)
    if df is not None:
        df.writeTo(f"{iceberg_catalog}.{gold_layer}.gst_distribution").createOrReplace()
    else:
        print("DataFrame is Not Available for Insertion into gst_distribution table")

def insert_into_pipeline_audit(spark,payload):
    schema = Schemas.Pipeline_AUDIT_Schema()
    df = spark.createDataFrame(payload,schema=schema)
    df.writeTo(f"{iceberg_catalog}.{bronze}.pipeline_audit").append()


def Update_pipeline_audit(status,request_id,spark):
    spark.sql(f"""
        UPDATE {iceberg_catalog}.{bronze}.pipeline_audit T
        SET T.pipeline_end_time=current_timestamp(),T.pipeline_stage_status='{status}',
        T.ingestion_timestamp=current_timestamp()
        WHERE T.Pipeline_Audit_ID='{request_id}'
    """)