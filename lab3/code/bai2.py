import pyspark
from pyspark import SparkContext, SparkConf

# initalize SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)

# load movies
def keep_id_genre(line):
    movie_id, _, genres = line.split(",")
    genres_list = genres.split("|")
    return movie_id, genres_list

movies_rdd = sc.textFile("hdfs://localhost:9000/movie_data/movies.txt").map(keep_id_genre)

# load ratings
def keep_id_rating(line):
    _, movie_id, rating, _ = line.split(",")
    return movie_id, float(rating)

ratings_rdd = sc.textFile("hdfs://localhost:9000/movie_data/ratings_*.txt").map(keep_id_rating)

# join movies and ratings
joined_rdd = movies_rdd.join(ratings_rdd)

# get ratings for each genre
def get_genre_ratings(line):
    movie_id = line[0]
    genres_list = line[1][0]
    rating = line[1][1]

    results = []
    for genre in genres_list:
        results.append((genre, (rating, 1)))

    return results

genre_rdd = joined_rdd.flatMap(get_genre_ratings)

# reduceByKey to get total ratings and counts
def get_total_ratings(value1, value2):
    total_rating, total_count = value1
    rating, count = value2
    return total_rating + rating, total_count + count

rating_totals = genre_rdd.reduceByKey(get_total_ratings)

# get average ratings
def get_avg_ratings(line):
    id, (total_rating, total_count) = line
    return id, (total_rating/total_count, total_count)

avg_ratings = rating_totals.map(get_avg_ratings).sortByKey()

for genre, (avg, count) in avg_ratings.collect():
    print(f"Genre: {genre}, Average ratings: {avg:.2f}")

sc.stop()