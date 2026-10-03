from Configs.Spark_Core import session,insertion,pipeline_audit
from Configs.API import Resolve_Params,API_HIT
from pyspark.sql.functions import col,explode,substr,to_date,collect_list,trunc,current_date,date_sub,count,lit,current_timestamp,concat,date_format,row_number,when
from pyspark.sql.types import StructType, StructField, DateType
from pyspark.sql.window import Window
from datetime import date,timedelta
iceberg_catalog = "AstroSight"
bronze="bronze"
import json



def backfill_missing_apis():
    spark = session.get_spark_session()
    # request_id = pipeline_audit.start_audit(pipeline_stage='BACKFILL_MISSING_API',pipeline_target_table='api_response',spark=spark)
    df_api = spark.table(f"{iceberg_catalog}.{bronze}.api_endpoints")\
              .filter(col("endpoint_type")=="scheduled")\
              .filter(col("is_active")=="Y")\
              .filter(col("api_name")=="NASA")
    df_con = spark.table(f"{iceberg_catalog}.{bronze}.api_backfill_control").filter(col("missing_apis").isNotNull()).orderBy(col("processed_date"),ascending=False).limit(2)
    df_modified = df_con.select("processed_date",explode(col("missing_apis")).alias("missing_api"))
    df_resp= spark.table(f"{iceberg_catalog}.{bronze}.api_response").filter(col("refreshed_to_silver")=='N').select(col("Entity_Requested"),to_date(col("Request_Params").substr(17, 10)).alias("processed_date"))
    df_new = df_modified.alias("m").join(df_resp.alias("r"),((col("m.processed_date")==col("r.processed_date"))&(col("m.missing_api")==col("r.Entity_Requested"))),"left_anti")
    df  = df_new.groupBy("processed_date").agg(
        collect_list("missing_api").alias("missing_apis")
    )
    
    try:
        for row in df.toLocalIterator():
            processed_date = row['processed_date']
            missing_apis = row['missing_apis']
            for api in missing_apis:
                api_url = df_api.filter(col("endpoint_name")==api).select("endpoint_url").first()[0]
                api_params = json.loads(df_api.filter(col("endpoint_name")==api).select("request_params").first()[0])
                if api_params['start_date']=='today':
                    api_params['start_date']=str(processed_date)
                if api_params['end_date']=='today':
                    api_params['end_date']=str(processed_date)
                data,status=API_HIT.get_url_response(url=api_url,additional_params=api_params)
                if status == 200:
                    payload = {
                        "URL_Endpoint": api_url,
                        "API_Request_Type": api,
                        "Entity_Requested": api,
                        "Request_Params":api_params,
                        "Raw_Api_Response": data,
                        "Response_status": status,
                        "error_msg": None
                    }
                else:
                    payload = {
                        "URL_Endpoint": api_url,
                        "API_Request_Type": api,
                        "Entity_Requested": api,
                        "Request_Params":api_params,
                        "Raw_Api_Response": None,
                        "Response_status": status,
                        "error_msg": data
                    }
                # print(payload)
                # insertion.insert_into_api_response(payload,spark)
                # pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)
    except Exception as e:
        print(f"Job Failed with error {e}")
        # pipeline_audit.end_audit(status='FAILED',request_id=request_id,spark=spark)


def Historical_Data():
    spark = session.get_spark_session()
    request_id = pipeline_audit.start_audit(pipeline_stage='HISTORICAL_MISSING',pipeline_target_table='api_response',spark=spark)
    temp_schema = StructType([
        StructField("processed_date",DateType(),False)
    ])
    active_month = None
    df_con = spark.table(f"{iceberg_catalog}.{bronze}.api_backfill_control").filter(col("missing_apis").isNotNull()).orderBy(col("processed_date"),ascending=False)
    df_api = spark.table(f"{iceberg_catalog}.{bronze}.api_endpoints").filter((col("api_name")=="NASA")&(col("is_active")=="Y")).agg(
        count("*").alias("total_active_api"),
        collect_list("endpoint_name").alias("missing_apis")
    )
    start_date = date.today()- timedelta(days=1)
    total_dates = []
    try:
        while active_month is None:
            first_date = start_date.replace(day=1)
            while start_date!=first_date:
                total_dates.append(start_date)
                start_date=start_date-timedelta(days=1)
            df_dates = spark.createDataFrame([(d,) for d in total_dates],schema=temp_schema)
            df_new = df_dates.alias("d").join(df_con.alias("c"),col("d.processed_date")==col("c.processed_date"),"left_anti")
            if df_new.isEmpty():
                first_date-timedelta(days=1)
            else:
                active_month="Y"

        if active_month=="Y":
            df_res = df_new.crossJoin(df_api).withColumn("processed_apis_count",lit(0)).withColumn("last_ingestion",current_timestamp()).withColumn("last_updated",lit(None))\
                        .select("processed_date","total_active_api","processed_apis_count","missing_apis","last_ingestion","last_updated")
            # df_res.show()
            insertion.merge_into_api_backfill_control_historical(df=df_res,spark=spark)
            pipeline_audit.end_audit(status='PASSED',request_id=request_id,spark=spark)
    except Exception as e:
        print(f"Job Failed with error {e}")
        pipeline_audit.end_audit(status='FAILED',request_id=request_id,spark=spark)

def CDC_Check():
    spark = session.get_spark_session()
    # request_id = pipeline_audit.start_audit(pipeline_stage='CDC_CHECK',pipeline_target_table='api_response',spark=spark)
    df_cont = spark.table(f"{iceberg_catalog}.{bronze}.api_backfill_control").filter(((col("last_checked").isNull())|(col("last_checked")<=col("last_updated")))&(col("missing_apis").isNull())).select("processed_date","last_checked")
    df_resp = spark.table(f"{iceberg_catalog}.{bronze}.api_response").filter((col("Request_Params").isNotNull())&(col("refreshed_to_silver")=="Y")).withColumn("processed_date",to_date(col("Request_Params").substr(17, 10))).select("processed_date","API_Request_Type","Raw_Api_Response","refreshed_timestamp","Request_Params","URL_Endpoint")

    df_new = df_cont.join(df_resp,"processed_date")
    window_spec = Window.partitionBy("processed_date","API_Request_Type").orderBy(col("refreshed_timestamp").desc())

    df_latest = df_new.filter(col("refreshed_timestamp").isNotNull()).withColumn("rn",row_number().over(window_spec)).filter(col("rn")==1).drop("rn").limit(5)
    for row in df_latest.toLocalIterator():
        data,status=API_HIT.get_url_response(url=row['URL_Endpoint'],additional_params=json.loads(row['Request_Params']))
        print(data)
    df_latest.select("last_checked").show(100,truncate=False)


if __name__ == "__main__":
    backfill_missing_apis()
    # Historical_Data()
    # CDC_Check()