catalog = "AstroSight"
bronze="bronze"
silver="silver"
gold="gold"

def create_bronze_tables(spark):
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{bronze}.api_response (
            request_id STRING,
            URL_Endpoint STRING,
            API_Request_Type STRING,
            Request_Params STRING,
            Entity_Requested STRING,
            Raw_Api_Response STRING,
            Response_status INT,
            refreshed_to_silver STRING,
            refreshed_timestamp TIMESTAMP,
            error_msg STRING,
            ingestion_timestamp TIMESTAMP
        )
    """
    spark.sql(create_sql)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{bronze}.api_endpoints(
            endpoint_id INT,
            api_name STRING,
            endpoint_name STRING,
            endpoint_url STRING,
            request_params STRING,
            pagination_supported STRING,
            is_active STRING,
            endpoint_type STRING
        )
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{bronze}.processing_error_log(
        Error_Id STRING,
        request_id STRING,
        source_layer STRING,
        target_layer STRING,
        source_table STRING,
        target_table STRING,
        error_message STRING,
        error_timestamp TIMESTAMP,
        status STRING
        )
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{bronze}.pipeline_watermark(
        process_name STRING,
        last_processed_request_id STRING,
        last_processed_timestamp TIMESTAMP,
        updated_timestamp TIMESTAMP
        )
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{bronze}.pipeline_audit(
        Pipeline_Audit_ID STRING,
        Pipeline_run_date DATE,
        pipeline_stage STRING,
        pipeline_target_table STRING,
        pipeline_start_time TIMESTAMP,
        pipeline_end_time TIMESTAMP,
        pipeline_stage_status STRING,
        pipeline_expected_records INT,
        pipeline_processed_records INT,
        ingestion_timestamp TIMESTAMP
        )
    """)

def create_silver_tables(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{silver}.neo_objects(
        Asteroid_Id STRING,
        Asteroid_Name STRING,
        absolute_magnitude DOUBLE,
        estimated_diameter_min_kms DOUBLE,
        estimated_diameter_max_kms DOUBLE,
        is_potentially_hazardous BOOLEAN,
        nasa_jpl_url STRING,
        feed_date DATE,
        ingestion_timestamp TIMESTAMP
        )
    """)

    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{silver}.neo_close_approaches(
            Approach_id STRING,
            Asteroid_Id STRING,
            close_approach_date_full TIMESTAMP,
            miss_distance_kms DOUBLE,
            miss_distance_miles DOUBLE,
            miss_distance_lunar DOUBLE,
            relative_velocity_kmph DOUBLE,
            relative_velocity_kmps DOUBLE,
            orbiting_body STRING,
            feed_date DATE,
            ingestion_timestamp TIMESTAMP
            )
        """)
    
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{silver}.gst_ids (
            gst_id STRING,
            start_time TIMESTAMP,
            gst_link STRING,
            submission_time TIMESTAMP,
            version_id INT,
            ingestion_timestamp TIMESTAMP
            )
            USING iceberg
        """)
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{silver}.gst_kp_details (
            gst_kp_id STRING,
            gst_id STRING,
            kp_observed_time TIMESTAMP,
            kp_index DOUBLE,
            kp_source STRING,
            ingestion_timestamp TIMESTAMP
            )
            USING iceberg
        """)
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{silver}.linked_events (
            linked_event_id STRING,
            source_id STRING,
            source_type STRING,
            activity_id STRING,
            activity_type STRING,
            ingestion_timestamp TIMESTAMP
            )
        USING iceberg
        """)
    
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{silver}.apod_details (
            apod_id STRING,
            feed_date DATE,
            apod_title STRING,
            apod_media_type STRING,
            apod_explanation STRING,
            apod_url STRING,
            apod_hd_url STRING,
            apod_copyright STRING,
            apod_service_version STRING,
            ingestion_timestamp TIMESTAMP
            )
            USING iceberg
        """)

def create_gold_tables(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold}.neo_summary(
        summary_date DATE,
        total_neo_count INT,
        potentially_hazardous_count INT,
        refresh_timestamp TIMESTAMP
        )""")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold}.neo_rankings(
        Asteroid_Id STRING,
        asteroid_name STRING,
        estimated_diameter_max_kms DOUBLE,
        miss_distance_kms DOUBLE,
        relative_velocity_kmph DOUBLE,
        close_approach_date_full TIMESTAMP,
        is_potentially_hazardous BOOLEAN,
        largest_rank INT,
        closest_rank INT,
        fastest_rank INT,
        refresh_timestamp TIMESTAMP
        )""")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold}.gst_summary(
            summary_date DATE,
            total_gst_count  INT,
            refresh_timestamp TIMESTAMP
        )
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold}.gst_rankings(
            gst_id STRING,
            strongest_rank INT,
            longest_rank INT,
            avg_kp INT,
            max_kp INT,
            storm_duration_hours INT,
            refresh_timestamp TIMESTAMP
        )
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{gold}.gst_distribution(
            severity_bucket STRING,
            gst_count INT,
            refresh_timestamp TIMESTAMP
        )
    """)
