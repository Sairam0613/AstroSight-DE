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





# spark.sql("""DELETE FROM  AstroSight.bronze.api_response t where request_id in ('1c54673c-a5e9-4a94-90ea-a31cd8f040b7',
# 'c5b2f492-0b3f-49e7-9f5c-0a70f2b55959',
# '09e80f97-2620-41d3-a629-ff1fec2200d7',
# '5893beaa-7001-4590-bff8-0163a4b7eadd',
# '09099967-df99-4a20-b68e-27b21f761637',
# 'e3be7fca-6cf7-46a9-80b9-55b4656a6683',
# 'f5eb3e93-b1b6-4031-839d-b8194c2eab1b',
# 'dd7b4ea1-2cad-4bf4-a795-b2f2318590df',
# 'c6a6e802-5f1d-4cc6-a553-c6f5933b4bf1',
# '1e6d22e6-0e78-4552-bdf3-ba1ca64f33ba',
# '7165f1d4-16f0-499e-8837-74110cc53876',
# '8118e8af-f06a-4e2f-8534-daed9bc79296')""")


# spark.sql("UPDATE AstroSight.bronze.api_response set refreshed_to_silver='N' where refreshed_to_silver='P' and  API_Request_Type='gst'")

# spark.sql("SELECT request_id,ingestion_timestamp,Request_Params,Entity_Requested,refreshed_to_silver,ingestion_timestamp FROM AstroSight.bronze.api_response t where t.refreshed_to_silver='N'").show(20,truncate=False)

# spark.sql("SELECT * from AstroSight.bronze.api_backfill_control ").show()

# spark.sql("SELECT * FROM AstroSight.bronze.api_endpoints").show()

spark.sql("DELETE FROM AstroSight.bronze.api_backfill_control where processed_date=DATE('2026-09-27')")

spark.sql("""
    INSERT INTO AstroSight.bronze.api_backfill_control 
    VALUES (
        date '2026-09-27', 
        5, 
        0, 
        array('neo', 'IPS', 'gst', 'cme', 'apod'), 
        CURRENT_TIMESTAMP(), 
        CURRENT_TIMESTAMP(),
        Null
    )
""")


# spark.sql("ALTER TABLE AstroSight.bronze.api_backfill_control add columns (last_checked TIMESTAMP)")

# spark.sql("TRUNCATE TABLE AstroSight.bronze.api_backfill_control")

spark.sql("SELECT * from AstroSight.bronze.api_backfill_control order by 1 desc").show(20)

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