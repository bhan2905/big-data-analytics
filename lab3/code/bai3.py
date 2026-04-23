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

# load users
def keep_id_gender(line):
    user_id, gender = line.split(",")[:2]
    return user_id, gender

users_rdd = sc.textFile("hdfs://localhost:9000/movie_data/users.txt").map(keep_id_gender)

# load ratings
def keep_id_rating(line):
    user_id, movie_id, rating = line.split(",")[:3]
    return user_id, (movie_id, float(rating))

ratings_rdd = sc.textFile("hdfs://localhost:9000/movie_data/ratings_*.txt").map(keep_id_rating)

# join users and ratings
joined_rdd = users_rdd.join(ratings_rdd)

# get movieId and gender ratings
def movie_gender_ratings(line):
    user_id = line[0]
    gender = line[1][0]
    movie_id = line[1][1][0]
    rating = line[1][1][1]

    return (movie_id, gender), (rating, 1)

gender_rdd = joined_rdd.map(movie_gender_ratings)

# reduceByKey to get total ratings and counts
def get_total_ratings(value1, value2):
    total_rating, total_count = value1
    rating, count = value2
    return total_rating + rating, total_count + count

rating_totals = gender_rdd.reduceByKey(get_total_ratings)

# get average ratings
def get_avg_ratings(line):
    key, (total_rating, total_count) = line
    return key, (total_rating/total_count, total_count)

avg_ratings = rating_totals.map(get_avg_ratings)

# group by movieId
def group_by_movie(line):
    (movie_id, gender), (avg, count) = line
    return movie_id, (gender, avg)

grouped_ratings = avg_ratings.map(group_by_movie).groupByKey().sortByKey()

# create a dict to title mapping
movies_dict = dict(movies_rdd.collect())

for movie_id, gender in grouped_ratings.collect():
    title = movies_dict.get(movie_id, "Unknown")
    gender_dict = dict(gender)
    parts = []
    for group in ["F", "M"]:
        if group in gender_dict:
            parts.append(f"{group}: {gender_dict[group]:.2f}")
        else:
            parts.append(f"{group}: N/A")
    print(f"ID: {movie_id}, Title: {title}: [{', '.join(parts)}]")

sc.stop()