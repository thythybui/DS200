import os
os.environ["PYSPARK_PYTHON"] = r"D:\DS200\DS200\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\DS200\DS200\Scripts\python.exe"

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

movies_rdd = sc.textFile("movies.txt")
ratings1_rdd = sc.textFile("ratings_1.txt")
ratings2_rdd = sc.textFile("ratings_2.txt")
users_rdd = sc.textFile("users.txt")   

ratings_rdd = ratings1_rdd.union(ratings2_rdd)

def parse_movies(line):
    movie_id, title, _ = line.split(",")
    return movie_id, title

movies_rdd = movies_rdd.map(parse_movies)

def parse_ratings(line):
    user_id, movie_id, score, _ = line.split(",")
    return user_id, (movie_id, float(score))

ratings_rdd = ratings_rdd.map(parse_ratings)

def parse_users(line):
    user_id, gender, _, _, _ = line.split(",")
    return user_id, gender

users_rdd = users_rdd.map(parse_users)


ratings_with_gender = ratings_rdd.join(users_rdd)


def map_to_movie_gender(row):
    user_id, ((movie_id, score), gender) = row
    return (movie_id, gender), (score, 1)

movie_gender_rdd = ratings_with_gender.map(map_to_movie_gender)

def reduce_func(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])

total_rdd = movie_gender_rdd.reduceByKey(reduce_func)

def compute_avg(row):
    (movie_id, gender), (total_score, count) = row
    return movie_id, gender, total_score / count, count

avg_rdd = total_rdd.map(compute_avg)

movie_title_map = movies_rdd.map(lambda x: (x[0], x[1]))

final_rdd = avg_rdd.map(lambda x: (x[0], (x[1], x[2], x[3]))) \
                   .join(movie_title_map)

with open("output_Bai3.txt", "w", encoding="utf-8") as f:
    for row in final_rdd.collect():
        movie_id, ((gender, avg_score, count), title) = row

        line = (
            f"MovieID: {movie_id} | "
            f"Title: {title} | "
            f"Gender: {gender} | "
            f"Average Score: {avg_score:.2f} | "
            f"Total Ratings: {count}"
        )

        print(line)
        f.write(line + "\n")