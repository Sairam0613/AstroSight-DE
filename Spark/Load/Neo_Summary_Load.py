from Configs.Spark_Core import session, tables,insertion,pipeline_audit

iceberg_catalog = "AstroSight"
silver_layer = "silver"
gold_layer = "gold"


def neo_summary_load():
    spark=session.get_spark_session()
    request_id = pipeline_audit.start_audit(pipeline_stage='SILVER_TO_GOLD',pipeline_target_table='neo_summary',spark=spark)
    tables.create_gold_tables(spark)
    
    df = spark.sql(f"""
        select current_date() as summary_date,
        count(*) as total_neo_count,
        sum(case when is_potentially_hazardous = 'true' then 1 else 0 end) as potentially_hazardous_count,
        current_timestamp() as refresh_timestamp
        from {iceberg_catalog}.{silver_layer}.neo_objects 
        """)
    insertion.insert_into_neo_summary(df,spark)
    pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)

if __name__ == "__main__":
    neo_summary_load()
