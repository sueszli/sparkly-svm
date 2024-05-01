from pyspark import SparkContext, SparkConf
from pyspark.rdd import RDD
import pathlib

import json
import re


# rdd programming: https://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds


conf = SparkConf().setAppName("chi2").setMaster("local[*]")  # use all cores
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
datapath = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"

stoppath = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
stopwords = sc.textFile(str(stoppath)).collect()
regex = r"[ \t\d()\[\]{}.!?,;:+=\-_\"\'~#@&*%€$§\/]+"

# tokenization, case folding, stopword removal
# fmt: off
terms_cat: RDD = sc.textFile(str(datapath)) \
                .map(json.loads) \
                .map(lambda x: (x["reviewText"], x["category"])) \
                .map(lambda x: ([t for t in re.split(regex, x[0].lower()) if t and t not in stopwords], x[1]))
# fmt: on
print(f"progress - terms_cat: {terms_cat.take(1)}")

# use broadcast variable to share cat_count across all nodes


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
