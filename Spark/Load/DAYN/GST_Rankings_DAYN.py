from Configs.Spark_Core import session, tables,insertion

iceberg_catalog = "AstroSight"
silver_layer = "silver"
gold_layer = "gold"

def gst_Rankings_DAYN():
    spark=session.get_spark_session()
    tables.create_gold_tables(spark)

    df = spark.sql(f"""
        with A as (
            select id.gst_id,ROUND(
                   ((UNIX_TIMESTAMP(MAX(kp_observed_time))-UNIX_TIMESTAMP(MAX(start_time))))/3600,2
                   ) as storm_duration_hours,
                   avg(kp_index) as avg_kp,
                   max(kp_index) as max_kp
                   from {iceberg_catalog}.{silver_layer}.gst_kp_details kp
        join {iceberg_catalog}.{silver_layer}.gst_ids id on id.gst_id=kp.gst_id
        where kp.gst_id in (select distinct kp.gst_id from {iceberg_catalog}.{silver_layer}.gst_kp_details kp
        join {iceberg_catalog}.{silver_layer}.gst_ids id on id.gst_id=kp.gst_id 
        where DATE(id.ingestion_timestamp)=CURRENT_DATE() or DATE(kp.ingestion_timestamp)=CURRENT_DATE())
        group by id.gst_id),
        B as (
        select A.*,
            dense_rank() over ( order by A.storm_duration_hours desc) as longest_rank,
            dense_rank() over ( order by A.max_kp desc) as strongest_rank,
            CURRENT_TIMESTAMP() as refresh_timestamp from A
            ),
        C as (
        select gst_id,strongest_rank,longest_rank,avg_kp,max_kp,storm_duration_hours,refresh_timestamp from B
        where longest_rank<=5 or strongest_rank <=5
        UNION ALL
        select gst_id,strongest_rank,longest_rank,avg_kp,max_kp,storm_duration_hours,refresh_timestamp
        from {iceberg_catalog}.{gold_layer}.gst_rankings),
        lr as ( select *,
        row_number() over (partition by gst_id order by refresh_timestamp desc) as latest_rn  from C ),
        D as (
        select gst_id,
        dense_rank() over ( order by storm_duration_hours desc) as longest_rank,
        dense_rank() over ( order by max_kp desc) as strongest_rank,
        avg_kp,max_kp,storm_duration_hours,CURRENT_TIMESTAMP() as refresh_timestamp
        from lr where latest_rn=1 )
        select * from D
          where longest_rank<=5 or strongest_rank <=5
    """)
    insertion.insert_into_gst_rankings(df,spark)

if __name__ == "__main__":
    gst_Rankings_DAYN()