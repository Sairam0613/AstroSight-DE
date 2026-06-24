from Configs.Spark_Core import session, tables,insertion

iceberg_catalog = "AstroSight"
silver_layer = "silver"
gold_layer = "gold"

def gst_distribution_DAY0():
    spark=session.get_spark_session()
    tables.create_gold_tables(spark)

    df = spark.sql(f"""
        with A as (
        select gst_id,max(kp_index) as maxkp
        from {iceberg_catalog}.{silver_layer}.gst_kp_details kp
        group by kp.gst_id),
        B as (
        select gst_id,
        CASE
            WHEN maxkp < 5.0 THEN 'G0'
            WHEN maxkp BETWEEN 5.0 AND 5.9 THEN 'G1'
            WHEN maxkp BETWEEN 6.0 AND 6.9 THEN 'G2'
            WHEN maxkp BETWEEN 7.0 AND 7.9 THEN 'G3'
            WHEN maxkp BETWEEN 8.0 AND 8.9 THEN 'G4'
            WHEN maxkp >= 9.0 THEN 'G5'
        END AS severity_bucket FROM A)
        select severity_bucket,count(*) as gst_count,CURRENT_TIMESTAMP() as refresh_timestamp from B
        group by severity_bucket
    """)
    insertion.insert_into_gst_distribution(df,spark=spark)

if __name__ == "__main__":
    gst_distribution_DAY0()