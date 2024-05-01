import pyspark
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

import pathlib
import re


sc = pyspark.SparkContext("local", "app")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

# read data in as RDD
datapath = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
rdd = spark.read.json(str(datapath)).rdd
text_cat: RDD = rdd.map(lambda x: (x.reviewText, x.category))

# tokenization, case folding, stopword removal
regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
stopwords = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
stopwords = sc.textFile(str(stopwords)).collect()
terms_cat: RDD = text_cat.map(lambda x: ([t for t in re.split(regex, x[0].lower()) if t and t not in stopwords], x[1]))
print(f"🟢 done with tokenization: {terms_cat.take(1)}")

N: int = terms_cat.count()
cat_count: RDD = terms_cat.flatMap(lambda x: [(x[1], 1) for _ in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore  # [(category, count)]
print(f"🟢 done with counting categories: {cat_count.take(1)}")
term_count: RDD = terms_cat.flatMap(lambda x: [(t, 1) for t in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore  # [(term, count)]
print(f"🟢 done with counting terms: {term_count.collect()}")
