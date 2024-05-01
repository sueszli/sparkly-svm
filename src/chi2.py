import pyspark
from pyspark.sql import SparkSession

import pathlib
import re
from collections import defaultdict


sc = pyspark.SparkContext("local", "app")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

# read data in as RDD
datapath = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
rdd = spark.read.json(str(datapath)).rdd
text_cat = rdd.map(lambda x: (x.reviewText, x.category))

# tokenization, case folding, stopword removal
regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
stopwords = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
stopwords = sc.textFile(str(stopwords)).collect()
terms_cat = text_cat.map(lambda x: ([t for t in re.split(regex, x[0].lower()) if t and t not in stopwords], x[1]))

N: int = terms_cat.count()
cat_count: defaultdict = terms_cat.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y).collectAsMap()  # type: ignore

print(N)
print(cat_count)
print(terms_cat.take(1))
