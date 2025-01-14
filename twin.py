# -*- coding: utf-8 -*-
"""WorkingVersionSpark.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1w2Iro-A-HDdr_6WJ_UAhrP6uz6f3xY0i
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, collect_list
from pyspark.sql import functions
from pyspark.sql.functions import  array_contains, array_position, array_min, lit, array
import pandas as pd
import random

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MovieTwin") \
    .getOrCreate()

# spark = SparkSession.builder.appName("MovieTwin").config("spark.executor.instances", "10").config("spark.executor.memory", "4g").config("spark.executor.cores", "2").getOrCreate()

"""## Data Loading and Pre-processing"""

# Commented out IPython magic to ensure Python compatibility.
# Load only the movieId
movies = spark.read.csv("ml-latest-small/movies.csv", header=True, inferSchema=True, schema="movieId INT")

# Load only the userId and movieId columns
# By loading in only the relevant data, we reduce loading time to 1/20!
ratings = spark.read.csv("ml-latest-small/ratings.csv", header=True, inferSchema=True, schema="userId INT, movieId INT")

# set variables from documentation - avoid some unnecessary data loading
# the number have been verified by loading the actual datasets

num_movies = 9742
num_users = 610

# num_movies = 86537
# num_users = 330975

"""**Consideration**: User IDs are not consecutive, neither are movie IDs. For our purposes, we do not care about actual movie IDs, as long as they are aligned so we can compute user similarity. However, we do care about actual user IDs in the final step. For now, we can pretend the user are indexed by consecutive integers, and later (when we get the top 100 pairs) convert those numerals back to actual user IDs using a dictionary.

**Loading Optimization**: Using the pivot_table function takes forever, likely due to the varying length of the list of movies. Anticipating a much larger dataset, I wrote a custom function to minimize loading time. It first creates an empty table of size num_users x num_movies, then compute the list of movies watched by each user, and map each movieId to a movieIndex, and finally filling the cell `[userIndex, movieIndex]`with 1.
"""

# Load only the 'movieId' column into a Pandas DataFrame
movies_pd = pd.read_csv("ml-latest-small/movies.csv", usecols=['movieId'])

movie_ids_list = movies_pd['movieId'].tolist()

# Print the first few elements of the list
print(movie_ids_list[:100])
print(len(movie_ids_list))

random.seed(42)
random.shuffle(movie_ids_list)
permutation = movie_ids_list
print(permutation)

def minhash(movies):
    hash_value = -1
    for i, movie_id in enumerate(permutation):
        if movie_id in movies:
            hash_value = i
            break
    return hash_value

def group_ratings_by_user(ratings):
    user_movies_df = ratings.groupBy('userId').agg(collect_list('movieId').alias('movies'))
    user_movies = {row.userId: row.movies for row in user_movies_df.collect()}
    return user_movies

def minhash_hash(movies):
    positions = [array_position(lit(permutation), movie_id) for movie_id in movies]
    return int(array_min(array([pos for pos in positions if pos.isNotNull()])) or float('inf'))

user_movies = group_ratings_by_user(ratings)
print(user_movies)
# user_movies_df = spark.createDataFrame([(user_id, movies) for user_id, movies in user_movies.items()], ['userId', 'movies'])
# user_movies_df.show(10)

hashed_user_movies_dict = {}
for user_id, movies in user_movies.items():
  hash_value = minhash(movies)
  if hash_value in hashed_user_movies_dict:
        hashed_user_movies_dict[hash_value].append(user_id)
  else:
      hashed_user_movies_dict[hash_value] = [user_id]
print(hashed_user_movies_dict)

def jaccard_similarity(set1, set2):
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union

from itertools import combinations
import heapq

top_pairs = []

for hash_value, user_ids in hashed_user_movies_dict.items():
    if len(user_ids) > 1:  # If more than one user hashed to the same value
        for pair in combinations(user_ids, 2):  # Generate pairwise combinations of user IDs
            user1_movies = user_movies[pair[0]]
            user2_movies = user_movies[pair[1]]
            similarity = jaccard_similarity(set(user1_movies), set(user2_movies))
            if len(top_pairs) < 100:
                heapq.heappush(top_pairs, (similarity, pair))
            else:
                heapq.heappushpop(top_pairs, (similarity, pair))
        print(hash_value)

print("Top 100 similar user pairs:")
for similarity, pair in sorted(top_pairs, reverse=True):
    print(f"Users {pair} with Jaccard similarity: {similarity}")