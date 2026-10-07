from Configs.Spark_Core import session,insertion,tables
from Spark.Transform import Neo_Tarnsformer,Neo_Approach_Transformer,Gst_Kp_Transformer,Gst_Transformer,apod_details_transformer,CME_Transformer,IPS_Transformer
from Spark.Load import Neo_Rankings_Load,Neo_Summary_Load
from Spark.Load.DAYN import GST_Rankings_DAYN,GST_Summary_DAYN,Neo_Summary_Load_DAYN,Neo_Rankings_Load_DAYN,CME_Activity_Score_DAYN,CME_SUMMARY_DAYN
from Spark.Load.DAY0 import GST_Summary_DAY0,Neo_Rankings_DAY0,Neo_Summary_Load_DAY0
from Configs.AWS import S3_TO_Bronze


def Neo(spark):
    passed_ids_1 = Neo_Tarnsformer.transform_neo_data()
    passed_ids_2 = Neo_Approach_Transformer.transform_neo_close_data()
    passed_ids = list(set(passed_ids_1).intersection(set(passed_ids_2)))

    failed_ids = list(set(passed_ids_1).symmetric_difference(set(passed_ids_2)))
    try:
        if passed_ids:
            insertion.Update_api_response_status(passed_ids, spark)
        if failed_ids:
            insertion.Mark_api_response_as_failed(failed_ids,spark)
    except Exception as e:
        print(f"Error updating API response status: {e}")
    Neo_Summary_Load_DAYN.neo_summary_load_DAYN()
    Neo_Rankings_Load_DAYN.Neo_Rankings()

def GST(spark):
    passed_ids_1 = Gst_Transformer.gst_transform_data()
    passed_ids_2 = Gst_Kp_Transformer.gst_kp_details()
    passed_ids = list(set(passed_ids_1).intersection(set(passed_ids_2)))
    failed_ids = list(set(passed_ids_1).symmetric_difference(set(passed_ids_2)))

    try:
        if passed_ids:
            insertion.Update_api_response_status(passed_ids, spark)
        if failed_ids:
            insertion.Mark_api_response_as_failed(failed_ids,spark)
    except Exception as e:
        print(f"Error updating API response status: {e}")
    GST_Rankings_DAYN.gst_Rankings_DAYN()
    GST_Summary_DAYN.gst_summary_DAY0()


def APOD(spark):
    passed_ids = apod_details_transformer.apod_details_transform()
    insertion.Update_api_response_status(request_id=passed_ids,spark=spark)

def CME(spark):
    passed_ids = CME_Transformer.transform_cme_data()
    insertion.Update_api_response_status(request_id=passed_ids,spark=spark)
    CME_Activity_Score_DAYN.cme_activity_score_DAYN()
    CME_SUMMARY_DAYN.cme_summary_DAYN()

def IPS(spark):
    passed_ids = IPS_Transformer.transform_ips_data()
    insertion.Update_api_response_status(request_id=passed_ids,spark=spark)

# today = date.today().strftime("%Y-%m-%d")

# # url = f"https://api.nasa.gov/neo/rest/v1/feed"

# # url_1 = f"https://api.nasa.gov/DONKI/GST"

# url_2 = f"https://api.nasa.gov/planetary/apod"

# url_3 = f"https://api.nasa.gov/DONKI/CME"

# url_4 = f"https://api.nasa.gov/DONKI/CMEAnalysis"

# url_5 = f"https://api.nasa.gov/DONKI/IPS"

# params = {
#     "start_date":today,
#     "end_date":today
# }

# response,status = get_url_response(url_5,params)
# print(response)





spark=session.get_spark_session()
# tables.create_bronze_tables(spark)
# spark=S3_TO_Bronze.get_spark()
# S3_TO_Bronze.Load_to_bronze(spark=spark)
# Neo(spark)
# GST(spark)
# APOD(spark)
# CME(spark=spark)
# IPS(spark=spark)

# tables.create_bronze_tables(spark=spark)

# spark.sql("SHOW TABLES IN AstroSight.bronze").show()





# spark.sql("""DELETE FROM  AstroSight.bronze.api_response t where request_id in ('7eeb8dd1-a74e-46e0-91c7-068bf1b36442')""")


# spark.sql("""UPDATE AstroSight.bronze.api_response set refreshed_to_silver='Y' where request_id in ('a4d87b6e-b438-4a46-924e-b0965063fac6')""")



# spark.sql("SELECT request_id,ingestion_timestamp,Request_Params,Entity_Requested,refreshed_to_silver,ingestion_timestamp FROM AstroSight.bronze.api_response t where t.refreshed_to_silver='N'").show(20,truncate=False)

# spark.sql("SELECT * from AstroSight.bronze.api_backfill_control ").show()

# spark.sql("SELECT * FROM AstroSight.bronze.api_endpoints").show()

# spark.sql("DELETE FROM AstroSight.bronze.api_backfill_control where processed_date=DATE('2026-09-27')")

# spark.sql("""
#     INSERT INTO AstroSight.bronze.api_backfill_control 
#     VALUES (
#         date '2026-09-27', 
#         5, 
#         0, 
#         array('neo', 'IPS', 'gst', 'cme', 'apod'), 
#         CURRENT_TIMESTAMP(), 
#         CURRENT_TIMESTAMP(),
#         Null
#     )
# """)


# spark.sql("ALTER TABLE AstroSight.bronze.api_backfill_control add columns (last_checked TIMESTAMP)")

# spark.sql("TRUNCATE TABLE AstroSight.bronze.api_backfill_control")

# spark.sql("SELECT * from AstroSight.bronze.api_backfill_control order by 1 desc").show(20)

# spark.sql("""
# UPDATE AstroSight.bronze.api_endpoints
# SET endpoint_url = 'https://ccmc.gsfc.nasa.gov/DONKI-API/get/IPS'
# WHERE api_name = 'NASA'
#   AND endpoint_name = 'IPS'
# """)

# spark.sql("""
# UPDATE AstroSight.bronze.api_endpoints
# SET endpoint_url = 'https://ccmc.gsfc.nasa.gov/DONKI-API/get/CME'
# WHERE api_name = 'NASA'
#   AND endpoint_name = 'cme'
# """)

# spark.sql("""
# UPDATE AstroSight.bronze.api_endpoints
# SET endpoint_url = 'https://ccmc.gsfc.nasa.gov/DONKI-API/get/GST'
# WHERE api_name = 'NASA'
#   AND endpoint_name = 'gst'
# """)

# spark.sql("""
# UPDATE AstroSight.bronze.api_response
# SET URL_Endpoint = 'https://ccmc.gsfc.nasa.gov/DONKI-API/get/IPS'
# WHERE API_Request_Type = 'IPS'
# """)

# spark.sql("""
# UPDATE AstroSight.bronze.api_response
# SET URL_Endpoint = 'https://ccmc.gsfc.nasa.gov/DONKI-API/get/CME'
# WHERE API_Request_Type = 'cme'
# """)

# spark.sql("""
# UPDATE AstroSight.bronze.api_response
# SET URL_Endpoint = 'https://ccmc.gsfc.nasa.gov/DONKI-API/get/GST'
# WHERE API_Request_Type = 'gst'
# """)

# spark.sql("""
# UPDATE AstroSight.bronze.api_response
# SET Request_Params = replace(
#     replace(Request_Params, '"startDate"', '"start_date"'),
#     '"endDate"', '"end_date"'
# )
# WHERE API_Request_Type = 'cme'
#   AND Request_Params IS NOT NULL
# """)

# spark.sql("""
# UPDATE AstroSight.bronze.api_endpoints
# SET Request_Params = replace(
#     replace(Request_Params, '"startDate"', '"start_date"'),
#     '"endDate"', '"end_date"'
# )
#  WHERE api_name = 'NASA'
#   AND endpoint_name = 'cme'
# """)

# spark.sql("SELECT request_id,Request_Params,API_Request_Type,ingestion_timestamp FROM AstroSight.bronze.api_response where refreshed_to_silver='N'").show(100,truncate=False)

# spark.sql("SELECT * from AstroSight.bronze.api_backfill_control order by 1 desc").show(20,truncate=False)




# spark.sql("SELECT URL_Endpoint,API_Request_Type,Request_Params,Entity_Requested from AstroSight.bronze.api_response ").show(truncate=False)

# spark.sql("SELECT * from AstroSight.bronze.api_response where API_Request_Type='gst' and refreshed_to_silver='P'").show(50,truncate=False)

# spark.sql(f"""
# SELECT DISTINCT (CAST(substr(request_params,17,10) AS DATE)) AS processed_date,request_id
# 		,api_request_type,request_params,refreshed_timestamp
# 		,ingestion_timestamp,response_status,refreshed_to_silver
# 	FROM AstroSight.bronze.api_response where request_params is not null and CAST(substr(request_params,17,10) AS DATE)=DATE('2026-08-31')
# """).show()

# spark.sql("update AstroSight.bronze.api_response set refreshed_to_silver='Y',refreshed_timestamp=current_timestamp() where request_id='66610e5d-8d11-4b77-95bf-1ed54dc1f810'")

spark.stop()