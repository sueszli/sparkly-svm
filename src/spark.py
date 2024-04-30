import pyspark
from pyspark.sql import SparkSession

import json
import pathlib


sc = pyspark.SparkContext("local", "app")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

# read json as RDD
datapath = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
rdd = spark.read.json(str(datapath)).rdd

# count number of reviews
count = rdd.count()
print(f"Number of reviews: {count}")
