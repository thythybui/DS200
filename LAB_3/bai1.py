import os
os.environ["PYSPARK_PYTHON"] = r"D:\DS200\DS200\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\DS200\DS200\Scripts\python.exe"

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

movies_rdd = sc.textFile("movies.txt")
ratings1_rdd = sc.textFile("ratings_1.txt")
ratings2_rdd = sc.textFile("ratings_2.txt")

ratings_rdd = ratings1_rdd.union(ratings2_rdd)

def drop_movies_rdd_col(item):
    movie_id, title, _ = item.split(",")
    return movie_id, title


movies_rdd = movies_rdd.map(drop_movies_rdd_col)


def drop_ratings_rdd_col(item):
    _, movie_id, score, _ = item.split(",")
    return movie_id, (float(score), 1)


ratings_rdd = ratings_rdd.map(drop_ratings_rdd_col)


def get_total_ratings(v1, v2):
    return (
        v1[0] + v2[0],
        v1[1] + v2[1]
    )


total_ratings_rdd = ratings_rdd.reduceByKey(get_total_ratings)


def get_avg_rating(row):
    movie_id, (score, count) = row
    return movie_id, (score / count, count)


avg_ratings_rdd = total_ratings_rdd.map(get_avg_rating)

result_rdd = movies_rdd.join(avg_ratings_rdd)

with open("output_Bai1.txt", "w", encoding="utf-8") as f:
    for row in result_rdd.collect():

        movie_id, (title, (avg_score, total_rating)) = row

        line = (
            f"MovieID: {movie_id} | "
            f"Title: {title} | "
            f"Average Score: {avg_score:.2f} | "
            f"Total Ratings: {total_rating}"
        )

        print(line) 
        f.write(line + "\n")