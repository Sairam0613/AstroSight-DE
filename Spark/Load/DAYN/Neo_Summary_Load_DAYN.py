from Configs.Spark_Core import session, tables,insertion,pipeline_audit

iceberg_catalog = "AstroSight"
silver_layer = "silver"
gold_layer = "gold"
bronze_layer = 'bronze'


def neo_summary_load_DAYN():
    spark=session.get_spark_session()
    request_id = pipeline_audit.start_audit(pipeline_stage='SILVER_TO_GOLD',pipeline_target_table='neo_summary',spark=spark)
    tables.create_gold_tables(spark)
    
    df = spark.sql(f"""
        select feed_date as summary_date,
        count(*) as total_neo_count,
        sum(case when is_potentially_hazardous = True then 1 else 0 end) as potentially_hazardous_count,
        current_timestamp as refresh_timestamp
        from {iceberg_catalog}.{silver_layer}.neo_objects
        where ingestion_timestamp >= (
        select max(t.pipeline_end_time) as last_refresh 
        from {iceberg_catalog}.{bronze_layer}.pipeline_audit t where t.pipeline_stage_status='PASSED'
        and t.pipeline_stage='SILVER_TO_GOLD'
        and t.pipeline_target_table='neo_objects'
        )
        group by feed_date
        """)
    insertion.insert_into_neo_summary(df,spark)
    pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)

if __name__ == "__main__":
    neo_summary_load_DAYN()
