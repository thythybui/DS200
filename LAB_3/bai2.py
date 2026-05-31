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
    movie_id, title, genres = item.split(",")
    return movie_id, genres.split("|")

movies_rdd = movies_rdd.map(drop_movies_rdd_col)

def drop_ratings_rdd_col(item):
    _, movie_id, score, _ = item.split(",")
    return movie_id, float(score)

ratings_rdd = ratings_rdd.map(drop_ratings_rdd_col)

joined_rdd = ratings_rdd.join(movies_rdd)

def explode_genres(row):
    movie_id, (rating, genres) = row
    return [(genre, (rating, 1)) for genre in genres]

genre_ratings_rdd = joined_rdd.flatMap(explode_genres)

def merge_values(v1, v2):
    return (
        v1[0] + v2[0],  
        v1[1] + v2[1]   
    )

genre_sum_rdd = genre_ratings_rdd.reduceByKey(merge_values)

def compute_avg(row):
    genre, (total, count) = row
    return genre, (total / count, count)

genre_avg_rdd = genre_sum_rdd.map(compute_avg)

with open("output_Bai2.txt", "w", encoding="utf-8") as f:
    for row in genre_avg_rdd.collect():

        genre, (avg_score, total_count) = row

        line = (
            f"Genre: {genre} | "
            f"Average Score: {avg_score:.2f} | "
            f"Total Ratings: {total_count}"
        )

        print(line)
        f.write(line + "\n")