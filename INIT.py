from datetime import date
from Configs.Spark_Core import session,tables



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

print(spark.conf.get("spark.dynamicAllocation.enabled"))
print(spark.conf.get("spark.executor.instances"))

print("Starting Data Insertion")
required_params = '{"start_date":"today","end_date":"today"}'
print("Data Read")
spark.sql(f"""INSERT INTO AstroSight.bronze.api_endpoints VALUES(1,'NASA','neo','{url}','{required_params}','Y','Y','scheduled')""")
print("Inserted 1 Record")
spark.sql(f"""INSERT INTO AstroSight.bronze.api_endpoints VALUES(2,'NASA','gst','{url_1}','{required_params}','Y','Y','scheduled')""")
print("Data Insertion Completed")

spark.stop()