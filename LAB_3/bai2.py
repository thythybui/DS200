import os
os.environ["PYSPARK_PYTHON"] = r"D:\DS200\DS200\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\DS200\DS200\Scripts\python.exe"

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

movies_rdd = sc.textFile("movies.txt")
ratings1_rdd = sc.textFile("ratings_1.txt")
ratings2_rdd = sc.textFile("ratings_2.txt")

ratings_rdd = ratings1_rdd.union(ratings2_rdd)

