from Configs.Spark_Core import session, tables,insertion
from Configs.Spark_Core import pipeline_audit

iceberg_catalog = "AstroSight"
silver_layer = "silver"
gold_layer = "gold"


def cme_summary_DAY0():
    spark=session.get_spark_session()
    request_id = pipeline_audit.start_audit(pipeline_stage='SILVER_TO_GOLD',pipeline_target_table='cme_summary',spark=spark)
    tables.create_gold_tables(spark)
    df = spark.sql(f"""
        with A as (
        select 
        cm.*,
        ca.is_most_accurate,
        ca.time21_5,
        ca.latitude,
        ca.longitude,
        ca.halfangle,
        ca.speed,
        ca.type,
        ca.featurecode,
        ca.levelofdata,
        ca.tilt,
        ca.speedmeasuredatheight,
        ca.submissiontime,
        rank() over (partition by ca.cme_id order by ca.submissiontime desc) as rn
        from {iceberg_catalog}.{silver_layer}.cme_ids cm
        join {iceberg_catalog}.{silver_layer}.cme_analysis ca
        on cm.cme_id=ca.cme_id
        --where date(cm.cme_starttime)=current_date-INTERVAL '1' day
        ),
        B as (
        select
        DATE(cme_starttime) as summary_date,
        count(cme_id) as total_cme_count,
        max(speed) as max_speed,
        max(halfangle) as max_width,
        sum(
        case when speed>=1000 then 1 else 0 end 
        ) as fast_cme_count,
        sum(
        case when halfangle=360 then 1 else 0 end 
        ) as halo_cme_count,
        avg(speed) as avg_speed,
        count(distinct nullif(trim(cme_sourcelocation), '')) as unique_source_locations
        from A where rn=1 and is_most_accurate=true
        group by DATE(cme_starttime)),
        C as (
        select  summary_date,
        sum(case when no_of_instruments>1 then 1 else 0 end) as multi_instrument_cme_count
        from (
        select ci.cme_id,date(cm.cme_starttime) as summary_date,
        count(distinct instrument_recorded) as no_of_instruments from {iceberg_catalog}.{silver_layer}.cme_instruments ci
        join {iceberg_catalog}.{silver_layer}.cme_ids cm
        on cm.cme_id=ci.cme_id
        --where date(cm.cme_starttime)=current_date-INTERVAL '1' day
        group by ci.cme_id,date(cm.cme_starttime)) group by summary_date
        ),
        D as (
        select DATE(cme_starttime) as summary_date,
        AVG(cme_activity_score) as activity_score from {iceberg_catalog}.{gold_layer}.cme_activity_score
        GROUP BY DATE(cme_starttime)
        )
        select 
        b.summary_date,
        b.total_cme_count as total_cme,
        round(b.max_speed,2) as max_speed,
        b.max_width,
        b.fast_cme_count,
        round(b.avg_speed,2) as avg_speed,
        b.halo_cme_count,
        c.multi_instrument_cme_count as multi_instrumental_confirmed,
        b.unique_source_locations as unique_source_location,
        round(d.activity_score,2) as activity_score,
        CASE
            WHEN d.activity_score <= 25 THEN 'LOW'
            WHEN d.activity_score <= 50 THEN 'MODERATE'
            WHEN d.activity_score <= 75 THEN 'HIGH'
            ELSE 'EXTREME'
        END AS activity_level
        from B b 
        join C c 
        on b.summary_date=c.summary_date
        join D d
        on b.summary_date=d.summary_date
        """)
    insertion.merge_into_cme_summary(df=df,spark=spark)
    pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)

if __name__ == "__main__":
    cme_summary_DAY0()