import pyspark
from pyspark import SparkContext, SparkConf

# initalize SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)

# load users
def keep_id_occ(line):
    user_id, _, _, occ_id = line.split(",")[:4]
    return user_id, occ_id

users_rdd = sc.textFile("hdfs://localhost:9000/movie_data/users.txt").map(keep_id_occ)

# load occupations
def read_occupation(line):
    occ_id, occ = line.split(",")
    return occ_id, occ

occupation_rdd = sc.textFile("hdfs://localhost:9000/movie_data/occupation.txt").map(read_occupation)

# load ratings
def keep_id_rating(line):
    user_id, _, rating = line.split(",")[:3]
    return user_id, float(rating)

ratings_rdd = sc.textFile("hdfs://localhost:9000/movie_data/ratings_*.txt").map(keep_id_rating)

# join users and ratings
joined_rdd = users_rdd.join(ratings_rdd)

# get occId and ratings
def occ_ratings(line):
    occ_id = line[1][0]
    rating = line[1][1]

    return occ_id, (rating, 1)

occ_rdd = joined_rdd.map(occ_ratings)

# reduceByKey to get total ratings and counts
def get_total_ratings(value1, value2):
    total_rating, total_count = value1
    rating, count = value2
    return total_rating + rating, total_count + count

rating_totals = occ_rdd.reduceByKey(get_total_ratings)

# get average ratings
def get_avg_ratings(line):
    occ_id, (total_rating, total_count) = line
    return occ_id, (total_rating/total_count, total_count)

avg_ratings = rating_totals.map(get_avg_ratings).sortBy(lambda x: int(x[0]))

# create a dict to occupation mapping
occ_dict = dict(occupation_rdd.collect())

for occ_id, (rating, count) in avg_ratings.collect():
    occ = occ_dict.get(occ_id, "Unknown")
    print(f"ID: {occ_id}, Occupation: {occ}, Average rating: {rating:.2f}, Count: {count}")

sc.stop()