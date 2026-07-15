from Configs.Spark_Core import session, tables,insertion
from Configs.Spark_Core import pipeline_audit

iceberg_catalog = "AstroSight"
silver_layer = "silver"
gold_layer = "gold"


def Neo_Rankings():
    spark=session.get_spark_session()
    request_id = pipeline_audit.start_audit(pipeline_stage='SILVER_TO_GOLD',pipeline_target_table='neo_rankings',spark=spark)
    tables.create_gold_tables(spark)
    df = spark.sql(f"""
        with A as (
        select * from (
        select 
        no.Asteroid_Id,
        no.asteroid_name,
        no.estimated_diameter_max_kms,
        ca.miss_distance_kms,
        ca.relative_velocity_kmph,
        ca.close_approach_date_full,
        no.is_potentially_hazardous,
        row_number() over (partition by no.is_potentially_hazardous order by no.estimated_diameter_max_kms desc) as largest_rank,
        row_number() over (partition by no.is_potentially_hazardous order by ca.miss_distance_kms asc) as closest_rank,
        row_number() over (partition by no.is_potentially_hazardous order by ca.relative_velocity_kmph desc) as fastest_rank,
        ca.ingestion_timestamp as refresh_timestamp
        from {iceberg_catalog}.{silver_layer}.neo_objects no
        join {iceberg_catalog}.{silver_layer}.neo_close_approaches ca
        on no.asteroid_id = ca.asteroid_id) ranked
        where largest_rank <=5 or closest_rank <=5 or fastest_rank <=5
        UNION ALL
        select 
            Asteroid_Id,
            asteroid_name,
            estimated_diameter_max_kms,
            miss_distance_kms,
            relative_velocity_kmph,
            close_approach_date_full,
            is_potentially_hazardous,
            largest_rank,
            closest_rank,
            fastest_rank,
            refresh_timestamp from {iceberg_catalog}.{gold_layer}.neo_rankings nr),
        B AS (
        select *,
        row_number() over (partition by Asteroid_Id order by refresh_timestamp desc) as latest_rn
        from A),
        C AS (
        SELECT Asteroid_Id,
        asteroid_name,
        estimated_diameter_max_kms,
        miss_distance_kms,
        relative_velocity_kmph,
        close_approach_date_full,
        is_potentially_hazardous,
        row_number() over (partition by is_potentially_hazardous order by estimated_diameter_max_kms desc) as largest_rank,
        row_number() over (partition by is_potentially_hazardous order by miss_distance_kms asc) as closest_rank,
        row_number() over (partition by is_potentially_hazardous order by relative_velocity_kmph desc) as fastest_rank,
        CURRENT_TIMESTAMP() as refresh_timestamp 
        FROM B where latest_rn =1)
        select * from C where largest_rank <=5 or closest_rank <=5 or fastest_rank <=5
        """)
    insertion.insert_into_neo_rankings(df,spark)
    pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)

if __name__ == "__main__":
    Neo_Rankings()