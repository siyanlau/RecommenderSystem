from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
from pyspark.sql import functions
from pyspark.sql.functions import col, lit, when
import pandas as pd

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MovieTwin") \
    .getOrCreate()

# Load only the movieId
movies = spark.read.csv("ml-latest-small/movies.csv", header=True, inferSchema=True, schema="movieId INT")

# Load only the userId and movieId columns
# By loading in only the relevant data, we reduce loading time to 1/20!
ratings = spark.read.csv("ml-latest-small/ratings.csv", header=True, inferSchema=True, schema="userId INT, movieId INT")

# set variables from documentation - avoid some unnecessary data loading
# the number have been verified by loading the actual datasets

num_movies = 9742 
num_users = 610

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


def load_data(ratings, movies):
  # create an empty dataframe which we later fill with ones
  user_movie_empty = pd.DataFrame(0, index=range(1, num_users + 1), columns=range(1, num_movies + 1))

  # create a movieId to movieIndex (0-inedxed) mapping 
  distinct_movie_ids = sorted(set(row.movieId for row in movies.collect()))
  movie_id_index_map = {movie_id: index for index, movie_id in enumerate(distinct_movie_ids)} # index starts from zero

  # create a dataframe representing the list of movies watched by each user
  user_movies = group_ratings_by_user(ratings)
  user_movies_df = spark.createDataFrame([(user_id, movies) for user_id, movies in user_movies.items()], ['userId', 'movies'])

  # For each user, iterate over their list of movies and set the corresponding column to 1
  for i, row in enumerate(user_movies_df.collect()):
    for movieId in row['movies']:
      position = movie_id_index_map[movieId]
      user_movie_empty.iloc[i, position] = 1

  return user_movie_empty


loaded_data = load_data(ratings, movies)

loaded_data.head(10)

test = spark.createDataFrame(loaded_data.head(5))
test.show()

# this function exploits the fact that ratings are sorted by userID already
# to find the set of movies rated by each user, we loop throught the rows until a different userID occurs.

