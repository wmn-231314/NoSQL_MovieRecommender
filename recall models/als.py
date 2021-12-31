import csv

import pandas as pd
import numpy as np

from pyspark.mllib.recommendation import ALS
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkSessionExample") \
    .master("local") \
    .getOrCreate()

sc = spark.sparkContext
rawUserData = sc.textFile("ratings_train.csv")
print(rawUserData.count())
print(type(rawUserData))
print(rawUserData.first())
rawRatings = rawUserData.map(lambda line: line.split(",")[:3])
print("&&&&&" + str(rawRatings.take(5)))
training_RDD = rawRatings.map(lambda x: (x[0], x[1], x[2]))
print(training_RDD.first())
print(training_RDD.count())

rank = 50
model = ALS.train(training_RDD, rank, seed=5, iterations=10, lambda_=0.01)
print(model.recommendProducts(59713, 2))

my_dict = {}
my_list = []

temp = rawRatings.map(lambda x: (x[0], x[1]))
my_dict = temp.collectAsMap()
for row in my_dict:
    result = {}
    result['UserId'] = int(row)
    movies = str()
    for item in model.recommendProducts(int(row), 250):
        movies += str(item[1])+"-"
    result['Movies'] = movies
    my_list.append(result)

with open('result333.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['UserId', 'Movies'])
    writer.writeheader()
    for i in range(len(my_list)):
        writer.writerow({'UserId': my_list[i]['UserId'],
                         'Movies': my_list[i]['Movies']})