

spark.sql(f"""
    INSERT INTO AstroSight.config.api_table_mapping
VALUES
('cme',4,'silver','cme_ids',1,'$[*]',NULL,'Y',CURRENT_TIMESTAMP,'N'),
('cme',4,'silver','cme_analysis',2,'$[*]','cmeAnalyses','Y',CURRENT_TIMESTAMP,'Y'),
('cme',4,'silver','cme_instruments',3,'$[*]','instruments','Y',CURRENT_TIMESTAMP,'Y')
""")


spark.sql(f"""
INSERT INTO AstroSight.config.api_column_mapping
VALUES
('cme','cme_ids','cme_id','activityID','STRING',1,'Y',CURRENT_TIMESTAMP),
('cme','cme_ids','cme_catalog','catalog','STRING',2,'Y',CURRENT_TIMESTAMP),
('cme','cme_ids','cme_starttime','startTime','TIMESTAMP',3,'Y',CURRENT_TIMESTAMP),
('cme','cme_ids','cme_sourcelocation','sourceLocation','STRING',4,'Y',CURRENT_TIMESTAMP),
('cme','cme_ids','cme_submissiontime','submissionTime','TIMESTAMP',5,'Y',CURRENT_TIMESTAMP),
('cme','cme_ids','cme_versionid','versionId','INT',6,'Y',CURRENT_TIMESTAMP),
('cme','cme_ids','cme_note','note','STRING',7,'Y',CURRENT_TIMESTAMP),
('cme','cme_ids','cme_link','link','STRING',8,'Y',CURRENT_TIMESTAMP)
""")\


spark.sql(f"""
INSERT INTO AstroSight.config.api_column_mapping
VALUES
('cme','cme_analysis','analysis_id',NULL,'STRING',1,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','cme_id','ROOT.activityID','STRING',2,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','is_most_accurate','isMostAccurate','BOOLEAN',3,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','time21_5','time21_5','TIMESTAMP',4,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','latitude','latitude','DOUBLE',5,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','longitude','longitude','DOUBLE',6,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','halfAngle','halfAngle','DOUBLE',7,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','speed','speed','DOUBLE',8,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','type','type','STRING',9,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','featureCode','featureCode','STRING',10,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','levelOfData','levelOfData','INT',11,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','tilt','tilt','DOUBLE',12,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','speedMeasuredAtHeight','speedMeasuredAtHeight','DOUBLE',13,'Y',CURRENT_TIMESTAMP),
('cme','cme_analysis','submissionTime','submissionTime','TIMESTAMP',14,'Y',CURRENT_TIMESTAMP)
""")

spark.sql(f"""
INSERT INTO AstroSight.config.api_column_mapping
VALUES
('cme','cme_instruments','instrument_id',NULL,'UUID',1,'Y',CURRENT_TIMESTAMP),
('cme','cme_instruments','cme_id','ROOT.activityID','STRING',2,'Y',CURRENT_TIMESTAMP),
('cme','cme_instruments','instrument_recorded','displayName','STRING',3,'Y',CURRENT_TIMESTAMP)
""")

spark.sql(f"""
INSERT INTO AstroSight.config.table_merge_mapping
VALUES
('cme_ids','silver','merge_into_cme_ids'),
('cme_analysis','silver','merge_into_cme_analysis'),
('cme_instruments','silver','merge_into_cme_instruments')
""")

spark.sql("UPDATE AstroSight.config.api_table_mapping set is_context_array='N' where target_table in ('cme_ids')")

spark.sql("UPDATE AstroSight.config.api_table_mapping set is_context_array='Y' where target_table in ('cme_analysis','cme_instruments')")

spark.sql("UPDATE AstroSight.config.api_column_mapping t set json_source_path='UUID',data_type='STRING' where t.target_column in ('instrument_id','analysis_id')")



-----------------

--IPS--

spark.sql("""
INSERT INTO AstroSight.bronze.api_endpoints
VALUES (
    5,
    'NASA',
    'IPS',
    'https://api.nasa.gov/DONKI/IPS',
    '{"start_date":"today","end_date":"today"}',
    'Y',
    'Y',
    'scheduled'
)
""")

spark.sql("""
INSERT INTO AstroSight.config.api_table_mapping
VALUES
('IPS',5, 'silver', 'ips_ids',1, '$[*]', NULL,'Y', CURRENT_TIMESTAMP, 'N'),
('IPS',5, 'silver', 'ips_instruments', 2, '$[*]', 'instruments', 'Y', CURRENT_TIMESTAMP, 'Y')
""")

spark.sql("""
INSERT INTO AstroSight.config.api_column_mapping
VALUES
('IPS','ips_ids','ips_id','activityID','STRING',1,'Y',CURRENT_TIMESTAMP),
('IPS','ips_ids','ips_catalog','catalog','STRING',2,'Y',CURRENT_TIMESTAMP),
('IPS','ips_ids','ips_location','location','STRING',3,'Y',CURRENT_TIMESTAMP),
('IPS','ips_ids','ips_eventtime','eventTime','TIMESTAMP',4,'Y',CURRENT_TIMESTAMP),
('IPS','ips_ids','ips_submissiontime','submissionTime','TIMESTAMP',5,'Y',CURRENT_TIMESTAMP),
('IPS','ips_ids','ips_versionid','versionId','INT',6,'Y',CURRENT_TIMESTAMP),
('IPS','ips_ids','ips_link','link','STRING',7,'Y',CURRENT_TIMESTAMP)
""")

spark.sql("""
INSERT INTO AstroSight.config.api_column_mapping
(api_name, target_table, target_column, json_source_path,
 data_type, column_order, is_active, ingestion_timestamp)
VALUES
('IPS','ips_instruments','ips_instrument_id',NULL,'UUID',1,'Y',CURRENT_TIMESTAMP),
('IPS','ips_instruments','ips_id','activityID','STRING',1,'Y',CURRENT_TIMESTAMP),
('IPS','ips_instruments','instrument_recorded','displayName','STRING',2,'Y',CURRENT_TIMESTAMP)
""")

spark.sql("UPDATE AstroSight.config.api_column_mapping set json_source_path='UUID',data_type='STRING' where target_table='ips_instruments' and target_column='ips_instrument_id'")

spark.sql("UPDATE AstroSight.config.api_column_mapping set json_source_path='ROOT.activityID' where target_table='ips_instruments' and target_column='ips_id'")

spark.sql(f"""
INSERT INTO AstroSight.config.table_merge_mapping
VALUES
('ips_ids','silver','merge_into_ips_ids'),
('ips_instruments','silver','merge_into_ips_instruments')
""")








--Update BackFill SQL Query --
WITH A
AS (
	SELECT count(*) AS total_active_api
	FROM AstroSight.bronze.api_endpoints t
	WHERE t.api_name = 'NASA'
		AND t.is_active = 'Y'
	)
	,B
AS (
	SELECT DISTINCT (CAST(substr(request_params,17,10) AS DATE)) AS processed_date
		,api_request_type
		,ingestion_timestamp
	FROM AstroSight.bronze.api_response
	WHERE response_status = 200 and request_params is not null and refreshed_to_silver='Y'
	ORDER BY 1 DESC
	)
	,C
AS (
	SELECT processed_date
		,total_active_api
		,count(DISTINCT api_request_type) AS processed_apis_count
		,max(ingestion_timestamp) AS last_ingestion
	FROM B
		,A
	GROUP BY processed_date
		,total_active_api
	)
	,D
AS (
	SELECT DISTINCT (CAST(substr(r.request_params,17,10) AS DATE)) AS processed_date
	FROM AstroSight.bronze.api_response r where r.request_params is not null and r.refreshed_to_silver='Y'
	)
	,E
AS (
	SELECT d.processed_date
		,e.endpoint_name
	FROM D d
		,AstroSight.bronze.api_endpoints e
	WHERE e.api_name = 'NASA'
		AND e.is_active = 'Y'
	)
	,F
AS (
	SELECT e.processed_date
		,e.endpoint_name
		,r.api_request_type
		,r.ingestion_timestamp
	FROM E e
	left JOIN AstroSight.bronze.api_response r ON r.api_request_type = e.endpoint_name
		AND CAST(substr(r.request_params,17,10) AS DATE) = e.processed_date
		AND r.response_status = 200
        and r.refreshed_to_silver='Y'
	WHERE r.api_request_type IS NULL
	ORDER BY 1 DESC
	)	,
	G
AS (
	SELECT processed_date
		,array_agg(endpoint_name) AS missing_apis
	FROM F
	GROUP BY processed_date
	)
SELECT c.processed_date
	,c.total_active_api
	,c.processed_apis_count
	,g.missing_apis
	,c.last_ingestion,
    current_timestamp() as last_updated,
    null as last_checked
FROM C c
LEFT JOIN G g ON c.processed_date = g.processed_date
ORDER BY 1 DESC;



-----------------------------------------------------------------------------------------------------

WITH p
     AS (SELECT DISTINCT ( Cast(Substr(t.request_params, 17, 10) AS DATE) ) AS
                         processed_date
         FROM   bronze.api_response t
         WHERE  t.ingestion_timestamp >= (SELECT
                Max(last_updated) AS max_timestamp
                                          FROM   bronze.api_backfill_control)),
     a
     AS (SELECT Count(*) AS total_active_api
         FROM   astrosight.bronze.api_endpoints t
         WHERE  t.api_name = 'NASA'
                AND t.is_active = 'Y'),
     b
     AS (SELECT DISTINCT ( Cast(Substr(request_params, 17, 10) AS DATE) ) AS
                         processed_date,
                         api_request_type,
                         ingestion_timestamp
         FROM   astrosight.bronze.api_response
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
         FROM   astrosight.bronze.api_response r
         WHERE  r.request_params IS NOT NULL
                AND Cast(Substr(r.request_params, 17, 10) AS DATE) IN (SELECT
                    processed_date
                                                                       FROM   p)
        ),
     e
     AS (SELECT d.processed_date,
                e.endpoint_name
         FROM   d d,
                astrosight.bronze.api_endpoints e
         WHERE  e.api_name = 'NASA'
                AND e.is_active = 'Y'),
     f
     AS (SELECT e.processed_date,
                e.endpoint_name,
                r.api_request_type,
                r.ingestion_timestamp
         FROM   e e
                left join astrosight.bronze.api_response r
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
       c.last_ingestion
FROM   c c
       left join g g
              ON c.processed_date = g.processed_date
ORDER  BY 1 DESC; 