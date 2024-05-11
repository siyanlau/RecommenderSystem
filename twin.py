from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
from pyspark.sql import functions
from pyspark.sql.functions import col, lit, when
import pandas as pd

# Initialize SparkSession
# spark = SparkSession.builder \
#     .appName("MovieTwin") \
#     .getOrCreate()
    
spark = SparkSession.builder.appName("MovieTwin").config("spark.executor.instances", "20").config("spark.executor.memory", "4g").config("spark.executor.cores", "2").getOrCreate()

# Load only the movieId
movies = spark.read.csv("ml-latest-small/movies.csv", header=True, inferSchema=True, schema="movieId INT")

# Load only the userId and movieId columns
# By loading in only the relevant data, we reduce loading time to 1/20!
ratings = spark.read.csv("ml-latest-small/ratings.csv", header=True, inferSchema=True, schema="userId INT, movieId INT")
ratings = ratings.repartition(10)
# set variables from documentation - avoid some unnecessary data loading
# the number have been verified by loading the actual datasets

num_movies = 9742 
num_users = 610

# num_movies = 86537
# num_users = 330975

ratings.show(10)
print("haha!")

def group_ratings_by_user(ratings):
    user_movies = {}
    current_user = None
    current_movies = []
    for row in ratings.collect():
        user_id = row['userId']
        movie_id = row['movieId']
        if user_id != current_user:
            if current_user is not None:
                user_movies[current_user] = current_movies
            current_user = user_id
            current_movies = [movie_id]
        else:
            current_movies.append(movie_id)
    if current_user is not None:
        user_movies[current_user] = current_movies
    return user_movies


from pyspark.ml.linalg import Vectors

def load_data(ratings, movies):
    # create a movieId to movieIndex (0-indexed) mapping
    distinct_movie_ids = sorted(set(row.movieId for row in movies.collect()))
    movie_id_index_map = {movie_id: index for index, movie_id in enumerate(distinct_movie_ids)}

    # create a dataframe representing the list of movies watched by each user
    user_movies = group_ratings_by_user(ratings)
    user_movies_df = spark.createDataFrame([(user_id, movies) for user_id, movies in user_movies.items()], ['userId', 'movies'])

    # Convert user-movie mapping to sparse vectors
    user_movie_sparse_vectors = user_movies_df.rdd.map(lambda row: (row.userId, sparse_vector_from_movies(row.movies, movie_id_index_map)))

    # Convert RDD to DataFrame
    user_movie_sparse_vectors = user_movie_sparse_vectors.toDF(['userId', 'movieVector'])
    
    return user_movie_sparse_vectors


def sparse_vector_from_movies(movies, movie_id_index_map):
    indices = [movie_id_index_map[movieId] for movieId in movies]
    values = [1] * len(indices)  # Assuming all entries are 1
    return Vectors.sparse(num_movies, indices, values)


loaded_data = load_data(ratings, movies)

num_partitions = loaded_data.rdd.getNumPartitions()

print("Number of partitions:", num_partitions)

loaded_data.head(5)

# test = spark.createDataFrame(loaded_data.head(5))
# test.show()
