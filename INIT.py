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


# today = date.today().strftime("%Y-%m-%d")

# url = f"https://api.nasa.gov/neo/rest/v1/feed"

# url_1 = f"https://api.nasa.gov/DONKI/GST"


# params = {
#     "start_date":today,
#     "end_date":today
# }
# response,status = get_url_response(url_1,params)
# print(response)


spark=session.get_spark_session()

# Update_linked_events.Update_linked_events()

# gst_transform()

# spark.table("AstroSight.bronze.PIPELINE_WATERMARK").show()
# spark.table("AstroSight.silver.linked_events").show()

# spark.table("AstroSight.silver.gst_ids").show()
# spark.table("AstroSight.silver.gst_kp_details").show()

# spark.table("AstroSight.gold.GST_Summary").show()
# spark.table("AstroSight.gold.gst_rankings").show()
# spark.table("AstroSight.gold.gst_distribution").show()

# spark.table("AstroSight.bronze.API_RESPONSE").filter(col("API_Request_Type")=="gst").select("Raw_Api_Response").show(1,truncate=False)

# Gst_Kp_Transformer.gst_kp_details()
spark.table("AstroSight.bronze.API_RESPONSE").filter(col("API_Request_Type")=="gst").select("request_id","refreshed_to_silver").show(truncate=False)


# spark.sql("UPDATE AstroSight.bronze.API_RESPONSE set refreshed_to_silver='N' where API_Request_Type='gst'")

# NeoWS_Extract.execute_scheduled_requests()

# spark.table("AstroSight.bronze.API_RESPONSE").filter(col("API_Request_Type")=="gst").select("Raw_Api_Response").show(truncate=False)

# spark.table("AstroSight.bronze.API_RESPONSE").select("request_id","ingestion_timestamp","refreshed_to_silver").show(truncate=False)
# spark.sql(f"DELETE FROM AstroSight.bronze.API_ENDPOINTS where endpoint_id =2")
# required_params = '{"start_date":"today","end_date":"today"}'
# spark.sql(f"""INSERT INTO AstroSight.bronze.API_ENDPOINTS VALUES(2,'NASA','gst','{url_1}','{required_params}','Y','Y','scheduled')""")

# spark.sql("SHOW TABLES In AstroSight.Silver").show()

# spark.sql("DROP TABLE IF EXISTS AstroSight.silver.neo_objects")
# spark.sql("DROP TABLE IF EXISTS AstroSight.silver.neo_close_approaches")
# spark.sql("DROP TABLE IF EXISTS AstroSight.gold.neo_rankings")
# spark.sql("DROP TABLE IF EXISTS AstroSight.gold.neo_summary")
# tables.create_bronze_tables(spark)
# tables.create_silver_tables(spark)
# tables.create_gold_tables(spark)


# spark.sql("DROP NAMESPACE IF EXISTS AstroSight.bronze")
# spark.sql("DROP NAMESPACE IF EXISTS AstroSight.silver")
# spark.sql("DROP NAMESPACE IF EXISTS AstroSight.gold")
# spark.sql("DROP NAMESPACE IF EXISTS AstroSight")

# spark.sql("SHOW NAMESPACES IN AstroSight").show()
# spark.sql("SHOW TABLES IN AstroSight.bronze").show()
# spark.table("AstroSight.bronze.api_endpoints").show()
# spark.table("AstroSight.bronze.api_response").show()
# spark.table("AstroSight.silver.neo_close_approaches").show()
# spark.table(f"AstroSight.silver.neo_objects").show()



