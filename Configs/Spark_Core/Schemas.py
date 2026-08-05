from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,DoubleType,BooleanType,DateType,LongType


def Bronze_api_response_schema():
    schema = StructType([
        StructField("request_id", StringType(), True),
        StructField("URL_Endpoint", StringType(), True),
        StructField("API_Request_Type", StringType(), True),
        StructField("Request_Params", StringType(), True),
        StructField("Entity_Requested", StringType(), True),
        StructField("Raw_Api_Response", StringType(), True),
        StructField("Response_status", IntegerType(), True),
        StructField("refreshed_to_silver", StringType(), True),
        StructField("refreshed_timestamp", TimestampType(), True),
        StructField("error_msg", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    return schema

def Neo_Objects_Schema():
    schema = StructType([
        StructField('Asteroid_Id',StringType(),False),
        StructField('Asteroid_Name',StringType(),False),
        StructField('absolute_magnitude',DoubleType(),True),
        StructField('estimated_diameter_min_kms',DoubleType(),True),
        StructField('estimated_diameter_max_kms',DoubleType(),True),
        StructField('is_potentially_hazardous',BooleanType(),False),
        StructField('nasa_jpl_url',StringType(),False),
        StructField('feed_date',DateType(), True),
        StructField('ingestion_timestamp',TimestampType(), True)
    ])
    return schema

def Neo_close_approach_Schema():
    schema = StructType([
        StructField('Approach_id',StringType(),False),
        StructField('Asteroid_Id',StringType(),False),
        StructField('close_approach_date_full',TimestampType(),False),
        StructField('miss_distance_kms',DoubleType(),True),
        StructField('miss_distance_miles',DoubleType(),True),
        StructField('miss_distance_lunar',DoubleType(),True),
        StructField('relative_velocity_kmph',DoubleType(),True),
        StructField('relative_velocity_kmps',DoubleType(),True),
        StructField('orbiting_body',StringType(),True),
        StructField('feed_date',DateType(), True),
        StructField('ingestion_timestamp',TimestampType(), True)
    ])
    return schema

def PROCESSING_ERROR_LOG_Schema():
    schema = StructType([
        StructField('error_id',StringType(),False),
        StructField('request_id',StringType(),False),
        StructField('source_layer',StringType(),False),
        StructField('target_layer',StringType(),True),
        StructField('source_table',StringType(),True),
        StructField('target_table',StringType(),True),
        StructField('error_message',StringType(),True),
        StructField('error_timestamp',TimestampType(),True),
        StructField('status',StringType(),True)
    ])
    return schema

def Neo_Summary_Schema():
    schema = StructType([
        StructField('summary_date',DateType(),False),
        StructField('total_neo_count',IntegerType(),False),
        StructField('potentially_hazardous_count',IntegerType(),False),
        StructField('refresh_timestamp',TimestampType(), False)
    ])
    return schema

def Neo_Rankings_Schema():
    schema = StructType([
        StructField('Asteroid_Id',StringType(),False),
        StructField('asteroid_name',StringType(),False),
        StructField('estimated_diameter_max_kms',DoubleType(),True),
        StructField('miss_distance_kms',DoubleType(),True),
        StructField('relative_velocity_kmph',DoubleType(),True),
        StructField('close_approach_date_full',TimestampType(),False),
        StructField('is_potentially_hazardous',BooleanType(),False),
        StructField('largest_rank',IntegerType(),True),
        StructField('closest_rank',IntegerType(),True),
        StructField('fastest_rank',IntegerType(),True),
        StructField('refresh_timestamp',TimestampType(), False)
    ])
    return schema

def GST_IDs_Schema():
    schema = StructType([
         StructField('gst_id',StringType(),False),
         StructField('start_time',TimestampType(),False),
         StructField('gst_link',StringType(),False),
         StructField('submission_time',TimestampType(),False),
         StructField('version_id',IntegerType(),False),
         StructField('ingestion_timestamp',TimestampType(),False)
    ])
    return schema

def GST_KP_DETAILS_Schema():
    schema = StructType([
        StructField('gst_kp_id',StringType(),False),
        StructField('gst_id',StringType(),False),
        StructField('kp_observed_time',TimestampType(),False),
        StructField('kp_index',DoubleType(),False),
        StructField('kp_source',StringType(),False),
        StructField('ingestion_timestamp',TimestampType(),False)
    ])
    return schema

def Linked_Events_Schema():
    schema = StructType([
        StructField('linked_event_id',StringType(),False),
        StructField('source_id',StringType(),False),
        StructField('source_type',StringType(),False),
        StructField('activity_id',StringType(),False),
        StructField('activity_type',StringType(),False),
        StructField('ingestion_timestamp',TimestampType(),False)
    ])
    return schema

def PIPELINE_WATERMARK_Schema():
    schema = StructType([
        StructField('process_name',StringType(),False),
        StructField('last_processed_request_id',StringType(),False),
        StructField('last_processed_timestamp',TimestampType(),False),
        StructField('updated_timestamp',TimestampType(),False)
    ])
    return schema

def GST_Summary_Schema():
    schema = StructType([
        StructField('summary_date',DateType(),False),
        StructField('total_gst_count',IntegerType(),False),
        StructField('refresh_timestamp',TimestampType(), False)
    ])
    return schema

def GST_Rankings_Schema():
    schema = StructType([
        StructField('gst_id',StringType(),False),
        StructField('strongest_rank',IntegerType(),False),
        StructField('longest_rank',IntegerType(),False),
        StructField('avg_kp',DoubleType(),False),
        StructField('max_kp',DoubleType(),False),
        StructField('storm_duration_hours',DoubleType(),False),
        StructField('refresh_timestamp',TimestampType(),False)
    ])
    return schema

def GST_Distribution_Schema():
    schema = StructType([
        StructField('severity_bucket',StringType(),False),
        StructField('gst_count',IntegerType(),False),
        StructField('refresh_timestamp',TimestampType(),False)
    ])
    return schema

def Pipeline_AUDIT_Schema():
    schema = StructType([
        StructField('Pipeline_Audit_ID',StringType(),False),
        StructField('Pipeline_run_date',DateType(),False),
        StructField('pipeline_stage',StringType(),False),
        StructField('pipeline_target_table',StringType(),False),
        StructField('pipeline_start_time',TimestampType(),False),
        StructField('pipeline_end_time',TimestampType()),
        StructField('pipeline_stage_status',StringType()),
        StructField('pipeline_expected_records',IntegerType()),
        StructField('pipeline_processed_records',IntegerType()),
        StructField('ingestion_timestamp',TimestampType(),False)
    ])
    return schema

def apod_details_schema():
    schema = StructType([
        StructField("apod_id", StringType(), True),
        StructField("feed_date", DateType(), True),
        StructField("apod_title", StringType(), True),
        StructField("apod_media_type", StringType(), True),
        StructField("apod_explanation", StringType(), True),
        StructField("apod_url", StringType(), True),
        StructField("apod_hd_url", StringType(), True),
        StructField("apod_copyright", StringType(), True),
        StructField("apod_service_version", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    return schema

def cme_ids_schema():
    schema = StructType([
        StructField("cme_id", StringType(), False),
        StructField("cme_catalog", StringType(), True),
        StructField("cme_starttime", TimestampType(), True),
        StructField("cme_sourcelocation", StringType(), True),
        StructField("cme_submissiontime", TimestampType(), True),
        StructField("cme_versionid", IntegerType(), True),
        StructField("cme_note", StringType(), True),
        StructField("cme_link", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    return schema

def cme_analysis_schema():
    schema = StructType([
        StructField("analysis_id", StringType(), False),
        StructField("cme_id", StringType(), False),
        StructField("is_most_accurate", BooleanType(), True),
        StructField("time21_5", TimestampType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("halfAngle", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("type", StringType(), True),
        StructField("featureCode", StringType(), True),
        StructField("imageType", StringType(), True),
        StructField("measurementTechnique", StringType(), True),
        StructField("note", StringType(), True),
        StructField("levelOfData", IntegerType(), True),
        StructField("tilt", DoubleType(), True),
        StructField("minorHalfWidth", DoubleType(), True),
        StructField("speedMeasuredAtHeight", DoubleType(), True),
        StructField("submissionTime", TimestampType(), True),
        StructField("link", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    return schema

def cme_instruments_schema():
    schema = StructType([
        StructField("instrument_id", StringType(), False),
        StructField("cme_id", StringType(), False),
        StructField("instrument_recorded", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    return schema

def cme_activity_score_schema():
    schema = StructType([
        StructField("cme_id", StringType(), True),
        StructField("cme_starttime", TimestampType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("halfAngle", DoubleType(), True),
        StructField('time21_5',TimestampType(),True),
        StructField('speed',DoubleType(),True),
        StructField("cme_activity_score", DoubleType(), True),
        StructField("cme_activity_level", StringType(), True),
        StructField("refresh_timestamp", TimestampType(), True)
    ])
    return schema

def cme_summary_schema():
    schema = StructType([
        StructField("summary_date", DateType(), True),
        StructField("total_cme", IntegerType(), True),
        StructField("max_speed", DoubleType(), True),
        StructField("max_width", DoubleType(), True),
        StructField("fast_cme_count", IntegerType(), True),
        StructField("avg_speed", DoubleType(), True),
        StructField("halo_cme_count", IntegerType(), True),
        StructField("multi_instrumental_confirmed", IntegerType(), True),
        StructField("unique_source_location", StringType(), True),
        StructField("activity_score", DoubleType(), True),
        StructField("activity_level", StringType(), True),
        StructField("refresh_timestamp", TimestampType(), True)
    ])
    return schema

def ips_ids_schema():
    schema = StructType([
        StructField("ips_id", StringType(), False),
        StructField("ips_catalog", StringType(), True),
        StructField("ips_location", StringType(), True),
        StructField("ips_eventtime", TimestampType(), True),
        StructField("ips_submissiontime", TimestampType(), True),
        StructField("ips_versionid", StringType(), True),
        StructField("ips_link", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    return schema

def ips_instruments_schema():
    schema = StructType([
        StructField("ips_instrument_id", StringType(), False),
        StructField("ips_id", StringType(), True),
        StructField("instrument_recorded", StringType(), True),
        StructField('ingestion_timestamp',TimestampType(),True)
    ])
    return schema