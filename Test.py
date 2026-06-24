from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("Hello AstroSight EMR")

spark.range(10).show()