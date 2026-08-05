from Configs.Spark_Core import session, tables,insertion
from Configs.Spark_Core import pipeline_audit

iceberg_catalog = "AstroSight"
silver_layer = "silver"
gold_layer = "gold"


def cme_activity_score_DAYN():
    spark=session.get_spark_session()
    request_id = pipeline_audit.start_audit(pipeline_stage='SILVER_TO_GOLD',pipeline_target_table='cme_activity_score_N',spark=spark)
    tables.create_gold_tables(spark)
    df = spark.sql(f"""
            WITH inc_cme_ids AS (
            -- Fetch cme_ids ingested today across all 3 silver source tables
            SELECT cme_id FROM {iceberg_catalog}.{silver_layer}.cme_ids WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
            UNION
            SELECT cme_id FROM {iceberg_catalog}.{silver_layer}.cme_analysis WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
            UNION
            SELECT cme_id FROM {iceberg_catalog}.{silver_layer}.cme_instruments WHERE DATE(ingestion_timestamp) = CURRENT_DATE()
        ),
        A as (
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
            row_number() over (partition by ca.cme_id order by ca.submissiontime desc) as rn
            from {iceberg_catalog}.{silver_layer}.cme_ids cm
            join {iceberg_catalog}.{silver_layer}.cme_analysis ca 
            on cm.cme_id=ca.cme_id
            WHERE cm.cme_id IN (SELECT cme_id FROM inc_cme_ids)
            ),
            B as (
            select 
            q.cme_id,
            q.cme_starttime,
            q.latitude,
            q.longitude,
            q.halfangle,
            q.time21_5,
            q.speed,
            case when q.speed < 400 then 5
                when q.speed between 400 and 599 then 15
                when q.speed between 600 and 799 then 25
                when q.speed between 800 and 1000 then 35
                when q.speed between 1000 and 1499 then 45
                when q.speed >=1500 then 50
            end as speed_rating,
            case when q.halfangle=360 then 30 else 0 end as halo_rating
            from A q where q.rn=1
            ),
            C as (
            select
            b.cme_id,
            b.cme_starttime,
            b.latitude,
            b.longitude,
            b.halfangle,
            b.time21_5,
            b.speed,
            b.speed_rating,
            b.halo_rating,
            case when count(cm.instrument_recorded)=1 then 8
                when count(cm.instrument_recorded)=2 then 15
                when count(cm.instrument_recorded)>=3 then 20
            end as instrument_rating
            from B b 
            left join {iceberg_catalog}.{silver_layer}.cme_instruments cm 
            on b.cme_id = cm.cme_id
            group by b.cme_id,
            b.cme_starttime,
            b.latitude,
            b.longitude,
            b.halfangle,
            b.time21_5,
            b.speed,
            b.speed_rating,
            b.halo_rating)
            select cme_id,cme_starttime,latitude,longitude,halfangle as halfAngle,time21_5,speed,
            CASE
                WHEN (speed_rating+halo_rating+instrument_rating) <= 25 THEN 'LOW'
                WHEN (speed_rating+halo_rating+instrument_rating) <= 50 THEN 'MODERATE'
                WHEN (speed_rating+halo_rating+instrument_rating) <= 75 THEN 'HIGH'
                ELSE 'EXTREME'
            END AS cme_activity_level,
            (speed_rating+halo_rating+instrument_rating) as cme_activity_score from C
        """)
    insertion.merge_into_cme_activity_score(df=df,spark=spark)
    pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)

if __name__ == "__main__":
    cme_activity_score_DAYN()