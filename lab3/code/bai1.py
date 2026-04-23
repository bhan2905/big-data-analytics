import pyspark
from pyspark import SparkContext, SparkConf

# initalize SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)

# load movies
def keep_id_title(line):
    movie_id, title = line.split(",")[:2]
    return movie_id, title

movies_rdd = sc.textFile("hdfs://localhost:9000/movie_data/movies.txt").map(keep_id_title)

# load ratings
def keep_id_rating(line):
    _, movie_id, rating, _ = line.split(",")
    return movie_id, (float(rating),1)

ratings_rdd = sc.textFile("hdfs://localhost:9000/movie_data/ratings_*.txt").map(keep_id_rating)

# reduceByKey to get total ratings and counts
def get_total_ratings(value1, value2):
    total_rating, total_count = value1
    rating, count = value2
    return total_rating + rating, total_count + count

rating_totals = ratings_rdd.reduceByKey(get_total_ratings)

# get average ratings
def get_avg_ratings(line):
    id, (total_rating, total_count) = line
    return id, (total_rating/total_count, total_count)

avg_ratings = rating_totals.map(get_avg_ratings).sortByKey()

# create a dict to title mapping
movies_dict = dict(movies_rdd.collect())

for movie_id, (avg, count) in avg_ratings.collect():
    title = movies_dict.get(movie_id, "Unknown")
    print(f"ID: {movie_id}, Title: {title}, Average ratings: {avg:.2f}, Count: {count}")

# filter movie with at least 5 ratings
const = 5

def filter_movies(line):
    _, (_, count) = line
    return count >= const

avg_ratings_filtered = avg_ratings.filter(filter_movies)

if not avg_ratings_filtered.isEmpty():
    highest_rated = avg_ratings_filtered.max(key=lambda line: line[1][0])
    movie_id, (avg_score, count) = highest_rated
    title = movies_rdd.lookup(movie_id)[0]

    print(f"Movie with the highest average rating with at least {const} ratings:")
    print(f"- ID: {movie_id}")
    print(f"- Title: {title}")
    print(f"- Average ratings: {avg_score:.2f}")
    print(f"- Count: {count}")
else:
    print("No movies found.")

sc.stop()