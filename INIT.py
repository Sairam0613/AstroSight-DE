from Configs.Spark_Core import session,insertion,tables
from Spark.Extract import NeoWS_Extract
from Spark.Transform import Neo_Tarnsformer,Neo_Approach_Transformer,Gst_Kp_Transformer,Gst_Transformer,apod_details_transformer
from Spark.Load import Neo_Rankings_Load,Neo_Summary_Load
from Spark.Load.DAYN import GST_Rankings_DAYN,GST_Summary_DAYN,Neo_Summary_Load_DAYN,Neo_Rankings_Load_DAYN
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

# today = date.today().strftime("%Y-%m-%d")

# # url = f"https://api.nasa.gov/neo/rest/v1/feed"

# # url_1 = f"https://api.nasa.gov/DONKI/GST"

# url_2 = f"https://api.nasa.gov/planetary/apod"


# params = {
#     # "start_date":today,
#     # "end_date":today
# }

# response,status = get_url_response(url_2,params)
# print(response)



spark=session.get_spark_session()
# tables.create_bronze_tables(spark)
# spark=S3_TO_Bronze.get_spark()
S3_TO_Bronze.Load_to_bronze(spark=spark)
Neo(spark)
GST(spark)
APOD(spark)

# spark.table("AstroSight.silver.apod_details").show()

# spark.table("AstroSight.bronze.api_response").filter(col("API_Request_Type")=="apod").select("request_id","refreshed_to_silver","refreshed_timestamp","ingestion_timestamp").show(truncate=False)
spark.stop()