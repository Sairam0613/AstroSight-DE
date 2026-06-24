from Configs.Spark_Core import session, tables,insertion

iceberg_catalog = "AstroSight"
silver_layer = "silver"
gold_layer = "gold"


def neo_summary_load():
    spark=session.get_spark_session()
    tables.create_gold_tables(spark)
    
    df = spark.sql(f"""
        select current_date() as summary_date,
        count(*) as total_neo_count,
        sum(case when is_potentially_hazardous = 'true' then 1 else 0 end) as potentially_hazardous_count,
        current_timestamp() as refresh_timestamp
        from {iceberg_catalog}.{silver_layer}.neo_objects 
        """)
    insertion.insert_into_neo_summary(df,spark)

if __name__ == "__main__":
    neo_summary_load()
