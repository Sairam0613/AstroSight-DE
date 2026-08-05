from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
# os.environ["PYSPARK_PYTHON"] = r"C:\Users\sairam1310\My Projects\Spark\AstroSight\.customvenv\Scripts\python.exe"
# os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\sairam1310\My Projects\Spark\AstroSight\.customvenv\Scripts\python.exe"
# os.environ["SPARK_CLASSPATH"] = (
#     r"C:\SparkJars\hadoop-aws-3.3.4.jar;"
#     r"C:\SparkJars\aws-java-sdk-bundle-1.12.262.jar"
# )
def get_environment_config():
    """
    Returns:
        AWS   -> Running in EMR Serverless
        LOCAL -> Everything else
    """
    if os.getenv("PLATFORM_TYPE")=="EMR_SERVERLESS":
        return "AWS"
    else :
        return "LOCAL"



def get_spark_session():
    env = get_environment_config()
    if env=='LOCAL':
        # spark = SparkSession.builder \
        #     .appName("AstroSight") \
        #     .master("local[2]") \
        #     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        #     .config("spark.sql.catalog.AstroSight", "org.apache.iceberg.spark.SparkCatalog") \
        #     .config("spark.sql.catalog.AstroSight.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
        #     .config("spark.sql.catalog.AstroSight.uri", "http://astrosight-iceberg-rest:8181") \
        #     .config("spark.sql.catalog.AstroSight.warehouse", "/project/Warehouse") \
        #     .config("spark.driver.memory", "1g") \
        #     .config("spark.executor.memory", "1g") \
        #     .config("spark.executor.memoryOverhead", "500m") \
        #     .config("spark.sql.session.timeZone","Asia/Kolkata") \
        #     .getOrCreate()
        # spark.sparkContext.setLogLevel("ERROR")
        # return spark
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
    elif env=="AWS":
        spark = SparkSession.builder \
            .appName("AstroSight") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.AstroSight", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.AstroSight.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.AstroSight.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.AstroSight.warehouse", "s3://astrosight-de-data/warehouse/") \
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

def create_namespaces(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS AstroSight.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS AstroSight.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS AstroSight.gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS AstroSight.config")
