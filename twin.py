from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
from pyspark.sql import functions as F

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MovieTwin") \
    .getOrCreate()

# Load ratings data
ratings = spark.read.csv("ratings.csv", header=True, inferSchema=True)

ratings.show(20)

# # Group ratings data by user ID to get the set of movies rated by each user
# user_movies = ratings.groupBy('userId').agg(F.collect_set('movieId').alias('movies'))

# # Cartesian join to compute all user pairs
# user_pairs = user_movies.alias('u1').crossJoin(user_movies.alias('u2'))

# # Filter out duplicate pairs and self-joins
# user_pairs = user_pairs.filter('u1.userId < u2.userId')

# # Compute Jaccard similarity coefficient between sets of rated movies
# user_pairs = user_pairs.withColumn('jaccard', F.size(F.array_intersect('u1.movies', 'u2.movies')) / F.size(F.array_union('u1.movies', 'u2.movies')))

# # Identify top 100 pairs with highest Jaccard similarity coefficient
# top_100_pairs = user_pairs.orderBy(F.desc('jaccard')).limit(100)

# # Output top 100 pairs of similar users
# top_100_pairs.show()

# Stop SparkSession
spark.stop()
