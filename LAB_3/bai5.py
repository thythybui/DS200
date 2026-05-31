import os
os.environ["PYSPARK_PYTHON"] = r"D:\DS200\DS200\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\DS200\DS200\Scripts\python.exe"

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

users_rdd = sc.textFile("users.txt")
ratings1_rdd = sc.textFile("ratings_1.txt")
ratings2_rdd = sc.textFile("ratings_2.txt")

ratings_rdd = ratings1_rdd.union(ratings2_rdd)

def parse_users(line):
    user_id, gender, age, occupation, _ = line.split(",")
    return user_id, occupation

users_rdd = users_rdd.map(parse_users)

def parse_ratings(line):
    user_id, movie_id, rating, _ = line.split(",")
    return user_id, (float(rating), 1)

ratings_rdd = ratings_rdd.map(parse_ratings)

ratings_with_occ = ratings_rdd.join(users_rdd)

def map_to_occ(row):
    user_id, ((rating, count), occupation) = row
    return occupation, (rating, count)

occ_rdd = ratings_with_occ.map(map_to_occ)


def reduce_func(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])

total_rdd = occ_rdd.reduceByKey(reduce_func)

def compute_avg(row):
    occupation, (total_rating, count) = row
    return occupation, total_rating / count, count

result_rdd = total_rdd.map(compute_avg)

with open("output_Bai5.txt", "w", encoding="utf-8") as f:
    for row in result_rdd.collect():
        occupation, avg, count = row

        line = (
            f"Occupation: {occupation} | "
            f"Average Rating: {avg:.2f} | "
            f"Total Ratings: {count}"
        )

        print(line)
        f.write(line + "\n")