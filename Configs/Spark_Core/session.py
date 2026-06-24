from pyspark.sql import SparkSession
import os
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
    print(env)
    spark = SparkSession.builder \
        .appName("AstroSight") \
        .master("local[2]") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.AstroSight", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.AstroSight.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
        .config("spark.sql.catalog.AstroSight.uri", "http://astrosight-iceberg-rest:8181") \
        .config("spark.sql.catalog.AstroSight.warehouse", "/project/Warehouse") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.sql.session.timeZone","Asia/Kolkata") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def create_namespaces(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")
