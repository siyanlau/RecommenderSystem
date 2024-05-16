from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ALS Recommendation System") \
    .getOrCreate()
    
data = spark.read.csv("ml-latest/ratings.csv", header=True, inferSchema=True, schema="userId INT")

data.tail(1)

spark.stop()