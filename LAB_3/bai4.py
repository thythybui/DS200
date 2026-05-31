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

def drop_user_rdd_col(item):
    user_id, _, age, _, _ = item.split(",")
    return user_id, age


users_rdd = users_rdd.map(drop_user_rdd_col)


def drop_ratings_rdd_col(item):
    user_id, movie_id, score, _ = item.split(",")
    return user_id, (movie_id, float(score))


ratings_rdd = ratings_rdd.map(drop_ratings_rdd_col)

ratings_with_age = ratings_rdd.join(users_rdd)


def map_to_movie_age(row):
    user_id, ((movie_id, score), age) = row
    return (movie_id, age), (score, 1)


movie_age_rdd = ratings_with_age.map(map_to_movie_age)


def reduce_func(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])


total_rdd = movie_age_rdd.reduceByKey(reduce_func)


def compute_avg(row):
    (movie_id, age), (total_score, count) = row
    return movie_id, age, total_score / count, count


avg_rdd = total_rdd.map(compute_avg)


movie_title_map = movies_rdd.map(lambda x: x.split(",")) \
                            .map(lambda x: (x[0], x[1]))


final_rdd = avg_rdd.map(lambda x: (x[0], (x[1], x[2], x[3]))) \
                   .join(movie_title_map)


with open("output_Bai4.txt", "w", encoding="utf-8") as f:
    for row in final_rdd.collect():
        movie_id, ((age, avg_score, count), title) = row

        line = (
            f"MovieID: {movie_id} | "
            f"Title: {title} | "
            f"Age: {age} | "
            f"Average Score: {avg_score:.2f} | "
            f"Total Ratings: {count}"
        )

        print(line)
        f.write(line + "\n")