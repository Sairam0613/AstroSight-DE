from Configs.Spark_Core import session, tables,insertion

iceberg_catalog = "AstroSight"
silver_layer = "silver"
gold_layer = "gold"

def gst_summary_DAY0():
    spark=session.get_spark_session()
    tables.create_gold_tables(spark)

    df = spark.sql(f"""
        select current_date() as summary_date,
        count(distinct gst_id) as total_gst_count,
        current_timestamp() as refresh_timestamp
        from {iceberg_catalog}.{silver_layer}.gst_ids
        where DATE(start_time) = current_date()
        """)
    insertion.insert_into_gst_summary(df,spark)

if __name__ == "__main__":
    gst_summary_DAY0()