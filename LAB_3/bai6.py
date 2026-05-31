import os
os.environ["PYSPARK_PYTHON"] = r"D:\DS200\DS200\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\DS200\DS200\Scripts\python.exe"

from pyspark import SparkContext
from datetime import datetime

sc = SparkContext.getOrCreate()

ratings1_rdd = sc.textFile("ratings_1.txt")
ratings2_rdd = sc.textFile("ratings_2.txt")

ratings_rdd = ratings1_rdd.union(ratings2_rdd)

def parse_ratings(line):
    user_id, movie_id, rating, timestamp = line.split(",")
    year = datetime.fromtimestamp(int(timestamp)).year
    return year, (float(rating), 1)

year_rdd = ratings_rdd.map(parse_ratings)

def reduce_func(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])

total_rdd = year_rdd.reduceByKey(reduce_func)

def compute_avg(row):
    year, (total_rating, count) = row
    return year, total_rating / count, count

result_rdd = total_rdd.map(compute_avg)

with open("output_Bai6.txt", "w", encoding="utf-8") as f:
    for row in result_rdd.collect():
        year, avg, count = row

        line = (
            f"Year: {year} | "
            f"Average Rating: {avg:.2f} | "
            f"Total Ratings: {count}"
        )

        print(line)
        f.write(line + "\n")