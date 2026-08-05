import json
import uuid
from datetime import datetime
from Configs.Spark_Core import Schemas,Schema_Validation
from pyspark.sql.functions import current_timestamp

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
    try:
        if request_id:
            print(f"Updating API response status to Y for request ids : {request_id}")
            ids = ",".join([f"'{id}'" for id in request_id])
            spark.sql(f"""
                UPDATE {iceberg_catalog}.{bronze}.api_response
                SET refreshed_to_silver = 'Y',
                refreshed_timestamp = current_timestamp()
                WHERE request_id in ({ids})
            """)
    except Exception as e:
        print("Update API response merge failed with error:",e)

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

def merge_into_apod_details(spark,payload):
    try:
        payload[0]['apod_id']=str(uuid.uuid4())
        schema = Schemas.apod_details_schema()
        df = spark.createDataFrame(payload,schema)
        df.createOrReplaceTempView("apod_details")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{silver_layer}.apod_details t
            USING apod_details s
            ON t.feed_date = s.feed_date AND t.apod_title = s.apod_title
            WHEN MATCHED THEN 
                UPDATE SET *
            WHEN NOT MATCHED THEN 
                INSERT *
        """)
    except Exception as e:
        print("Merge Failed with Error:",e)

def merge_into_cme_ids(payload,spark):
    try:
        print("Starting Merge for CME_IDS")
        schema = Schemas.cme_ids_schema()
        df = spark.createDataFrame(payload,schema)
        df = df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        df.createOrReplaceTempView("cme_ids")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{silver_layer}.cme_ids AS target
            USING cme_ids AS source
            ON target.cme_id = source.cme_id

            WHEN MATCHED THEN
            UPDATE SET
                cme_catalog = source.cme_catalog,
                cme_starttime = source.cme_starttime,
                cme_sourcelocation = source.cme_sourcelocation,
                cme_submissiontime = source.cme_submissiontime,
                cme_versionid = source.cme_versionid,
                cme_note = source.cme_note,
                cme_link = source.cme_link,
                ingestion_timestamp = source.ingestion_timestamp
            WHEN NOT MATCHED THEN
            INSERT *
        """)
        print("Merge Completed for CME_IDS")
    except Exception as e:
            print("Merge Failed with Error for CME_IDS:",e)

def merge_into_cme_analysis(payload,spark):
    try:
        print("MERGE STARTED FOR CME_ANALYSIS")
        schema = Schemas.cme_analysis_schema()
        df = spark.createDataFrame(payload,schema)
        df = df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        df.createOrReplaceTempView("cme_analysis")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{silver_layer}.cme_analysis AS target
            USING cme_analysis AS source
            ON target.cme_id = source.cme_id and target.time21_5 = source.time21_5 and target.submissionTime = source.submissionTime

            WHEN MATCHED THEN
            UPDATE SET
                is_most_accurate = source.is_most_accurate,
                latitude = source.latitude,
                longitude = source.longitude,
                halfAngle = source.halfAngle,
                speed = source.speed,
                type = source.type,
                featureCode = source.featureCode,
                levelOfData = source.levelOfData,
                tilt = source.tilt,
                speedMeasuredAtHeight = source.speedMeasuredAtHeight,
                submissionTime = source.submissionTime,
                ingestion_timestamp = source.ingestion_timestamp

            WHEN NOT MATCHED THEN
            INSERT *
        """)
        print('MERGE COMPLETED FOR CME_ANALYSIS')
    except Exception as e:
            print("Merge Failed for CME_ANALYSIS with Error:",e)

def merge_into_cme_instruments(payload,spark):
    try:
        print("MERGE STARTED FOR CME_INSTRUMENTS")
        schema = Schemas.cme_instruments_schema()
        df = spark.createDataFrame(payload,schema)
        df = df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        df.createOrReplaceTempView("cme_instruments")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{silver_layer}.cme_instruments AS target
            USING cme_instruments AS source
            ON target.cme_id = source.cme_id
            AND target.instrument_recorded = source.instrument_recorded

            WHEN MATCHED THEN
            UPDATE SET
                ingestion_timestamp = source.ingestion_timestamp
            WHEN NOT MATCHED THEN
            INSERT *
        """)
        print("MERGE COMPLETED FOR CME_INSTRUMENTS")
    except Exception as e:
            print("Merge Failed for CME_INSTRUMENTS with Error:",e)

def merge_into_cme_activity_score(df,spark):
    try:
        schema = Schemas.cme_activity_score_schema()
        df = df.withColumn(
                            "refresh_timestamp",
                            current_timestamp()
                        )
        df = Schema_Validation.validate_and_cast_schema(df, schema)
        df.createOrReplaceTempView("cme_activity_score")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{gold_layer}.cme_activity_score t
            USING cme_activity_score s
            ON t.cme_id = s.cme_id
            WHEN MATCHED THEN 
                UPDATE SET *
            WHEN NOT MATCHED THEN 
                INSERT *
            """)
    except Exception as e:
        print("Merge For CME_ACTIVITY_SCORE failed with error:",e)

def merge_into_cme_summary(df,spark):
    try:
        schema = Schemas.cme_summary_schema()
        df = df.withColumn(
                            "refresh_timestamp",
                            current_timestamp()
                        )
        df = Schema_Validation.validate_and_cast_schema(df, schema)
        df.createOrReplaceTempView("cme_summary")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{gold_layer}.cme_summary t
            USING cme_summary s
            ON t.summary_date = s.summary_date
            WHEN MATCHED THEN 
                UPDATE SET *
            WHEN NOT MATCHED THEN 
                INSERT *
        """)

    except Exception as e:
        print("Merge Failed for CME_SUMMARY with error",e)

def merge_into_ips_ids(payload,spark):
    try:
        print("MERGE STARTED FOR IPS_IDS")
        schema = Schemas.ips_ids_schema()
        df = spark.createDataFrame(payload,schema)
        df = df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        df.createOrReplaceTempView("ips_ids")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{silver_layer}.ips_ids AS target
            USING ips_ids AS source
            ON target.ips_id = source.ips_id
            WHEN MATCHED THEN
            UPDATE SET
                target.ips_catalog        = source.ips_catalog,
                target.ips_location       = source.ips_location,
                target.ips_eventtime      = source.ips_eventtime,
                target.ips_submissiontime = source.ips_submissiontime,
                target.ips_versionid      = source.ips_versionid,
                target.ips_link           = source.ips_link,
                target.ingestion_timestamp = source.ingestion_timestamp
            WHEN NOT MATCHED THEN
            INSERT (
                ips_id,
                ips_catalog,
                ips_location,
                ips_eventtime,
                ips_submissiontime,
                ips_versionid,
                ips_link,
                ingestion_timestamp
            )
            VALUES (
                source.ips_id,
                source.ips_catalog,
                source.ips_location,
                source.ips_eventtime,
                source.ips_submissiontime,
                source.ips_versionid,
                source.ips_link,
                source.ingestion_timestamp
            )
        """)
        print("MERGE COMPLETED FOR IPS_IDS")
    except Exception as e:
            print("Merge Failed for IPS_IDS with Error:",e)


def merge_into_ips_instruments(payload,spark):
    try:
        print("MERGE STARTED FOR IPS_INSTRUMENTS")
        schema = Schemas.ips_instruments_schema()
        df = spark.createDataFrame(payload,schema)
        df = df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        df.createOrReplaceTempView("ips_instruments")
        spark.sql(f"""
            MERGE INTO {iceberg_catalog}.{silver_layer}.ips_instruments AS target
            USING ips_instruments AS source
            ON target.ips_id = source.ips_id 
            AND target.instrument_recorded = source.instrument_recorded
            WHEN MATCHED THEN
            UPDATE SET
                target.ips_instrument_id = source.ips_instrument_id,
                target.ingestion_timestamp = source.ingestion_timestamp
            WHEN NOT MATCHED THEN
            INSERT (
                ips_instrument_id,
                ips_id,
                instrument_recorded,
                ingestion_timestamp
            )
            VALUES (
                source.ips_instrument_id,
                source.ips_id,
                source.instrument_recorded,
                source.ingestion_timestamp
            )
        """)
        print("MERGE COMPLETED FOR IPS_INSTRUMENTS")
    except Exception as e:
            print("Merge Failed for IPS_INSTRUMENTS with Error:",e)