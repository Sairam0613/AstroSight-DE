import requests
from datetime import date
from Configs.Spark_Core import session,tables,insertion,Update_linked_events
from Configs.API.API_HIT import get_url_response
from Spark.Extract import NeoWS_Extract
from Spark.Transform import Neo_Tarnsformer,Neo_Approach_Transformer,Gst_Transformer,Gst_Kp_Transformer
from Spark.Load import Neo_Rankings_Load,Neo_Summary_Load
from pyspark.sql.functions import col
from Spark.Load.DAY0 import GST_Summary_DAY0,GST_Rankings_DAY0,GST_Distribution_DAY0
import os

def gst_transform():
    sucessful_ids_1 = Gst_Transformer.gst_transform_data()
    sucessful_ids_2 = Gst_Kp_Transformer.gst_kp_details()
    passed_ids = list(set(sucessful_ids_1).intersection(set(sucessful_ids_2)))
    failed_ids = list(set(sucessful_ids_1).symmetric_difference(set(sucessful_ids_2)))
    if passed_ids:
        insertion.Update_api_response_status(passed_ids, spark)
    if failed_ids:
        insertion.Mark_api_response_as_failed(failed_ids,spark)
    GST_Summary_DAY0.gst_summary_DAY0()
    GST_Rankings_DAY0.gst_Rankings_DAY0()
    GST_Distribution_DAY0.gst_distribution_DAY0()


today = date.today().strftime("%Y-%m-%d")

url = f"https://api.nasa.gov/neo/rest/v1/feed"

url_1 = f"https://api.nasa.gov/DONKI/GST"


# params = {
#     "start_date":today,
#     "end_date":today
# }
# response,status = get_url_response(url_1,params)
# print(response)


spark=session.get_spark_session()

session.create_namespaces(spark=spark)
tables.create_bronze_tables(spark=spark)
tables.create_silver_tables(spark=spark)
tables.create_gold_tables(spark=spark)

# required_params = '{"start_date":"today","end_date":"today"}'
# spark.sql(f"""INSERT INTO AstroSight.bronze.API_ENDPOINTS VALUES(1,'NASA','neo','{url}','{required_params}','Y','Y','scheduled')""")
# spark.sql(f"""INSERT INTO AstroSight.bronze.API_ENDPOINTS VALUES(2,'NASA','gst','{url_1}','{required_params}','Y','Y','scheduled')""")

spark.sql("SHOW CATALOGS").show(truncate=False)
print("Warehouse:",spark.conf.get("spark.sql.catalog.AstroSight.warehouse","NOT FOUND"))




spark.stop()