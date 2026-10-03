from Configs.Spark_Core import session,insertion,pipeline_audit
from Configs.API import Resolve_Params,API_HIT
from pyspark.sql.functions import col,max,get_json_object
iceberg_catalog = "AstroSight"
bronze="bronze"
import json

def update_missing_api():
    spark = session.get_spark_session()
    request_id = pipeline_audit.start_audit(pipeline_stage='UPDATE_API_CONTROL',pipeline_target_table='api_response',spark=spark)
    try:
        df = spark.sql("""
        WITH p
            AS (SELECT DISTINCT ( Cast(Substr(t.request_params, 17, 10) AS DATE) ) AS
                                processed_date
                FROM   AstroSight.bronze.api_response t
                WHERE  t.ingestion_timestamp >= (SELECT
                        Max(last_updated) AS max_timestamp
                                                FROM   AstroSight.bronze.api_backfill_control)),
            a
            AS (SELECT Count(*) AS total_active_api
                FROM   AstroSight.bronze.api_endpoints t
                WHERE  t.api_name = 'NASA'
                        AND t.is_active = 'Y'),
            b
            AS (SELECT DISTINCT ( Cast(Substr(request_params, 17, 10) AS DATE) ) AS
                                processed_date,
                                api_request_type,
                                ingestion_timestamp
                FROM   AstroSight.bronze.api_response
                WHERE  response_status = 200
                        AND request_params IS NOT NULL
                        AND Cast(Substr(request_params, 17, 10) AS DATE) IN (SELECT
                            processed_date
                                                                            FROM   p)
                ORDER  BY 1 DESC),
            c
            AS (SELECT processed_date,
                        total_active_api,
                        Count(DISTINCT api_request_type) AS processed_apis_count,
                        Max(ingestion_timestamp)         AS last_ingestion
                FROM   b,
                        a
                GROUP  BY processed_date,
                        total_active_api),
            d
            AS (SELECT DISTINCT ( Cast(Substr(r.request_params, 17, 10) AS DATE) ) AS
                                processed_date
                FROM   AstroSight.bronze.api_response r
                WHERE  r.request_params IS NOT NULL
                        AND Cast(Substr(r.request_params, 17, 10) AS DATE) IN (SELECT
                            processed_date
                                                                            FROM   p)
                ),
            e
            AS (SELECT d.processed_date,
                        e.endpoint_name
                FROM   d d,
                        AstroSight.bronze.api_endpoints e
                WHERE  e.api_name = 'NASA'
                        AND e.is_active = 'Y'),
            f
            AS (SELECT e.processed_date,
                        e.endpoint_name,
                        r.api_request_type,
                        r.ingestion_timestamp
                FROM   e e
                        left join AstroSight.bronze.api_response r
                            ON r.api_request_type = e.endpoint_name
                                AND Cast(Substr(r.request_params, 17, 10) AS DATE) =
                                    e.processed_date
                                AND r.response_status = 200
                WHERE  r.api_request_type IS NULL
                        AND Cast(Substr(r.request_params, 17, 10) AS DATE) IN (SELECT
                            processed_date
                                                                            FROM   p)
                ORDER  BY 1 DESC),
            g
            AS (SELECT processed_date,
                        Array_agg(endpoint_name) AS missing_apis
                FROM   f
                GROUP  BY processed_date)
        SELECT c.processed_date,
            c.total_active_api,
            c.processed_apis_count,
            g.missing_apis,
            c.last_ingestion,
            current_timestamp() as last_updated
        FROM   c c
            left join g g
                    ON c.processed_date = g.processed_date
        ORDER  BY 1 DESC
        """)

        insertion.merge_into_api_backfill_control(df=df,spark=spark)
        pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)
    except Exception as e:
        print("Failed with Error:",e)
        pipeline_audit.end_audit(status='FAILED',request_id=request_id,spark=spark)


if __name__ == "__main__":
    update_missing_api()