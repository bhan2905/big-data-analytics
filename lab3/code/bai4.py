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
def get_age_group(age):
    age = int(age)
    if age <= 18: return "0-18"
    elif age <= 35: return "18-35"
    elif age <= 50: return "35-50"
    else: return "50+"

def keep_id_age(line):
    user_id, _, age = line.split(",")[:3]
    return user_id, get_age_group(age)

users_rdd = sc.textFile("hdfs://localhost:9000/movie_data/users.txt").map(keep_id_age)

# load ratings
def keep_id_rating(line):
    user_id, movie_id, rating = line.split(",")[:3]
    return user_id, (movie_id, float(rating))

ratings_rdd = sc.textFile("hdfs://localhost:9000/movie_data/ratings_*.txt").map(keep_id_rating)

# join users and ratings
joined_rdd = users_rdd.join(ratings_rdd)

# get movieId and age group ratings
def movie_age_ratings(line):
    user_id = line[0]
    age_group = line[1][0]
    movie_id = line[1][1][0]
    rating = line[1][1][1]

    return (movie_id, age_group), (rating, 1)

age_rdd = joined_rdd.map(movie_age_ratings)

# reduceByKey to get total ratings and counts
def get_total_ratings(value1, value2):
    total_rating, total_count = value1
    rating, count = value2
    return total_rating + rating, total_count + count

rating_totals = age_rdd.reduceByKey(get_total_ratings)

# get average ratings
def get_avg_ratings(line):
    key, (total_rating, total_count) = line
    return key, (total_rating/total_count, total_count)

avg_ratings = rating_totals.map(get_avg_ratings)

# group by movieId
def group_by_movie(line):
    (movie_id, age_group), (avg, count) = line
    return movie_id, (age_group, avg)

grouped_ratings = avg_ratings.map(group_by_movie).groupByKey().sortByKey()

# create a dict to title mapping
movies_dict = dict(movies_rdd.collect())

for movie_id, age in grouped_ratings.collect():
    title = movies_dict.get(movie_id, "Unknown")
    age_dict = dict(age)
    parts = []
    for group in ["0-18", "18-35", "35-50", "50+"]:
        if group in age_dict:
            parts.append(f"{group}: {age_dict[group]:.2f}")
        else:
            parts.append(f"{group}: N/A")
    print(f"ID: {movie_id}, Title: {title}: [{', '.join(parts)}]")

sc.stop()