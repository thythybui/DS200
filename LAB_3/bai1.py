from pyspark import SparkContext

sc = SparkContext.getOrCreate()

movies_rdd = sc.textFile("movies")
occupation_rdd = sc.textFile("movies")
ratings_rdd = sc.textFile("ratings")
users_rdd = sc.textFile("users")

def drop_movies_rdd_col(item: str):
    movie_id, title, _ = item.split(",")
    return movie_id, title

movies_rdd = movies_rdd.map(drop_movies_rdd_col)

def drop_ratings_rdd_col(item: str):
    _, movie_id, score, _ = item.split(",")
    return movie_id, [float(score), 1]

ratings_rdd = ratings_rdd.map(drop_ratings_rdd_col)

def get_total_ratings(value_1, value_2):
    score_1, total_rating_1 = value_1
    score_2, total_rating_2 = value_2
    return score_1 + score_2, total_rating_1 + total_rating_2

total_ratings_rdd = ratings_rdd.reduceByKey(get_total_ratings)

def get_avg_rating(row: tuple):
    movie_id, (score, count) = row
    avg_score = score/count
    return movie_id, (avg_score, count)

avg_ratings_rdd = total_ratings_rdd.map(get_avg_rating)

result_rdd = movies_rdd.join(avg_ratings_rdd)

for row in result_rdd.collect():
    _, title, (avg_score, total_rating) = row
    print(f"Title: {title} - Average score: {avg_score} - Total ratings: {total_rating}")
