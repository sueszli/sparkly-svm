from pyspark import SparkContext, SparkConf

import argparse
import pathlib
import re
import os


conf = SparkConf().setAppName("chi2").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# rdd programming: https://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds


datapath = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
text_cat = sc.textFile(str(datapath)).map(lambda x: (x["reviewText"], x["category"]))  # type: ignore
print(f"progress - text_cat: {text_cat.take(1)}")

# text_cat: RDD = spark.read.json(str(datapath)).rdd.map(lambda x: (x["reviewText"], x["category"]))


# # tokenization, case folding, stopword removal
# regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
# stoppath = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
# stopwords = sc.textFile(str(stoppath)).collect()
# terms_cat: RDD = text_cat.map(lambda x: ([t for t in re.split(regex, x[0].lower()) if t and t not in stopwords], x[1]))
# text_cat.unpersist()
# del text_cat
# print(f"progress - tokenized: {terms_cat.take(1)}")


# # chi2 calculation
# N: int = terms_cat.count()
# print(f"progress - N: {N}")

# # [(cat, count), ...]
# cat_count: RDD = terms_cat.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)  # type: ignore
# print(f"progress - cat_count: {cat_count.take(1)}")

# # [(term, count), ...]
# term_count: RDD = terms_cat.flatMap(lambda x: [(t, 1) for t in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore
# print(f"progress - term_count: {term_count.take(1)}")

# # [((term, cat), count), ...]
# term_cat_count: RDD = terms_cat.flatMap(lambda x: [((t, x[1]), 1) for t in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore
# print(f"progress - term_cat_count: {term_cat_count.take(1)}")
