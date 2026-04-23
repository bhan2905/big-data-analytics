import pyspark
from pyspark import SparkContext, SparkConf
from datetime import datetime

# initalize SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)

# get year from timestamp
def get_year(timestamp):
    return datetime.fromtimestamp(int(timestamp)).year

# load ratings
def keep_year_rating(line):
    _, _, rating, time = line.split(",")
    return get_year(time), (float(rating),1)

ratings_rdd = sc.textFile("hdfs://localhost:9000/movie_data/ratings_*.txt").map(keep_year_rating)

# reduceByKey to get total ratings and counts
def get_total_ratings(value1, value2):
    total_rating, total_count = value1
    rating, count = value2
    return total_rating + rating, total_count + count

rating_totals = ratings_rdd.reduceByKey(get_total_ratings)

# get average ratings
def get_avg_ratings(line):
    year, (total_rating, total_count) = line
    return year, (total_rating/total_count, total_count)

avg_ratings = rating_totals.map(get_avg_ratings).sortBy(lambda x: int(x[0]))

for year, (rating, count) in avg_ratings.collect():
    print(f"Year: {year}, Average rating: {rating:.2f}, Count: {count}")

sc.stop()