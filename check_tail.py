from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ALS Recommendation System") \
    .getOrCreate()
    
data = spark.read.csv("ml-latest/ratings", header=True, inferSchema=True, schema="userId INT")
data.tail(10)

spark.stop()