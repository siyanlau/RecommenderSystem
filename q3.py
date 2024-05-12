# -*- coding: utf-8 -*-
"""Recommendation_Q3.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1qPWFOzluPjQdddD9ZPMIRsXQW7hjoawA

# Movie Recommendation System

## Alternating Least Squares
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.sql import functions as F
import random
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, ArrayType
from pyspark.sql.functions import udf, col, sum, explode
import sys

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ALS Recommendation System") \
    .getOrCreate()
if len(sys.argv) < 2:
    arg = "big"
arg = sys.argv[1]

path = "ml-latest-small/ratings.csv" if arg == "small" else "ml-latest/ratings.csv"

data = spark.read.csv(path, header=True, inferSchema=True, schema="userId INT, movieId INT, rating FLOAT")
data.show()

train_ratio = 0.7
validation_ratio = 0.2
test_ratio = 0.1

# partitioning *one* user
def partition_ratings(user_id, ratings_list):
    total_ratings = len(ratings_list)
    train_count = int(total_ratings * train_ratio)
    validation_count = int(total_ratings * validation_ratio)
    test_count = total_ratings - train_count - validation_count

    # Generate random indices for each set
    indices = list(range(total_ratings))
    random.seed(42)
    random.shuffle(indices)
    train_indices = indices[:train_count]
    validation_indices = indices[train_count:train_count + validation_count]
    test_indices = indices[train_count + validation_count:]

    # Partition the ratings based on the selected indices
    train_set = [(user_id, movie_id, rating) for (movie_id, rating) in [ratings_list[i] for i in train_indices]]
    validation_set = [(user_id, movie_id, rating) for (movie_id, rating) in [ratings_list[i] for i in validation_indices]]
    test_set = [(user_id, movie_id, rating) for (movie_id, rating) in [ratings_list[i] for i in test_indices]]
    print(len(train_set))
    print(len(validation_set))
    print(len(test_set))

    return train_set, validation_set, test_set

def train_val_test_split(data):
  grouped_ratings = data.groupby('userId').agg(F.collect_list(F.struct('movieId', 'rating')).alias('ratings'))

  schema = StructType([
    StructField("train", ArrayType(StructType([
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("rating", FloatType(), False)
    ])), False),
    StructField("validation", ArrayType(StructType([
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("rating", FloatType(), False)
    ])), False),
    StructField("test", ArrayType(StructType([
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("rating", FloatType(), False)
    ])), False)
  ])

  # UDF to apply partition_ratings function to each row of the DataFrame
  partition_udf = udf(lambda user_id, ratings_list: partition_ratings(user_id, ratings_list), schema)

  # This will be cleaned by garbage collector after the function returns
  partitioned_ratings_df = grouped_ratings.withColumn('partitioned_data', partition_udf(col('userId'), col('ratings')))

  # Extract the three partitions into separate columns
  train_data = partitioned_ratings_df.selectExpr("partitioned_data['train'] AS train_data").withColumn("exploded", explode('train_data')).drop('train_data').select(col('exploded.userId').alias('userId'),
                                     col('exploded.movieId').alias('movieId'),
                                     col('exploded.rating').alias('rating'))
  
  test_data = partitioned_ratings_df.selectExpr("partitioned_data['test'] AS test_data").withColumn("exploded", explode('test_data')).drop('test_data').select(col('exploded.userId').alias('userId'),
                                     col('exploded.movieId').alias('movieId'),
                                     col('exploded.rating').alias('rating'))
  
  validation_data = partitioned_ratings_df.selectExpr("partitioned_data['validation'] AS validation_data").withColumn("exploded", explode('validation_data')).drop('validation_data').select(col('exploded.userId').alias('userId'),
                                     col('exploded.movieId').alias('movieId'),
                                     col('exploded.rating').alias('rating'))

  return train_data, test_data, validation_data


train_data, test_data, validation_data = train_val_test_split(data)

print(type(train_data))

num_rows = train_data.count()
print("Length of train_data:", num_rows)

# first_row = train_data.select('train_data').first()
# num_elements = len(first_row[0])
# print("Length of train_data[0]", num_elements)

train_data.show(10)