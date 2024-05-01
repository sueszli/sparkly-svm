import pyspark
from pyspark.sql import SparkSession
import pathlib
import re

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

print(terms_cat.take(1))
