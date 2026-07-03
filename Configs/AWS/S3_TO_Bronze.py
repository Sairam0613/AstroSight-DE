from pyspark.sql import SparkSession
import boto3
from datetime import date
from pyspark.sql.functions import col
from Configs.Spark_Core import insertion
import json

def get_spark():
    spark = SparkSession.builder \
            .appName("AstroSight") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.AstroSight", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.AstroSight.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.AstroSight.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.AstroSight.warehouse", "s3a://astrosight-de-data/warehouse/") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.profile.ProfileCredentialsProvider") \
            .config("spark.sql.catalog.AstroSight.glue.region","ap-south-1") \
            .config("spark.dynamicAllocation.enabled","false")\
            .config("spark.executor.instances","1")\
            .config("spark.executor.cores","1")\
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.session.timeZone","Asia/Kolkata") \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")    
    return spark  

def get_bucket_name():
    return "astrosight-de-data"

def get_folders(Prefix):
    s3 = boto3.client("s3")
    Bucket = get_bucket_name()
    response = s3.list_objects_v2(
        Bucket = Bucket,
        Prefix=Prefix,
        Delimiter = "/"
    )
    return response

def Load_to_bronze(spark):

    folders_response = get_folders("NASA/")
    for folder in folders_response.get("CommonPrefixes",[]):
        api_name = folder['Prefix'].split("/")[1]
        todays_date = date.today().strftime("%Y-%m-%d")
        file_path = f"NASA/{api_name}/{todays_date}/"
        files_response = get_folders(file_path)
        for file in files_response.get("Contents",[]):
            file_name = file['Key'].split("/")[-1]
            endpoint_url = spark.table("AstroSight.bronze.api_endpoints").filter(col("endpoint_name")==api_name).first()['endpoint_url']
            data = spark.read.text(f"s3a://astrosight-de-data/{file['Key']}").first()['value']
            raw_response = json.loads(data)
            if file_name == "api_response.json":
                payload = {
                    "URL_Endpoint": endpoint_url,
                    "API_Request_Type": api_name,
                    "Entity_Requested": api_name,
                    "Raw_Api_Response": raw_response,
                    "Response_status": 200,
                    "error_msg": None
                }
            elif file_name == "error.json":
                payload = {
                    "URL_Endpoint": endpoint_url,
                    "API_Request_Type": api_name,
                    "Entity_Requested": api_name,
                    "Raw_Api_Response": None,
                    "Response_status": 400,
                    "error_msg": raw_response
                }
            else:
                print(f"Skipping for file {file_name}")
                continue
            insertion.insert_into_api_response(payload,spark)
