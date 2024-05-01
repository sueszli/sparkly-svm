import pyspark
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

import argparse
import pathlib
import re
import os


os.environ["PYTHONHASHSEED"] = "0"  # enable lookup of the same term in different partitions

sc = pyspark.SparkContext("local", "app")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)


datapath = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
text_cat: RDD = spark.read.json(str(datapath)).rdd.map(lambda x: (x["reviewText"], x["category"]))


# tokenization, case folding, stopword removal
regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
stoppath = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
stopwords = sc.textFile(str(stoppath)).collect()
terms_cat: RDD = text_cat.map(lambda x: ([t for t in re.split(regex, x[0].lower()) if t and t not in stopwords], x[1]))
text_cat.unpersist()
del text_cat
print(f"progress - tokenized: {terms_cat.take(1)}")


# chi2 calculation
N: int = terms_cat.count()
print(f"progress - N: {N}")

# [(cat, count), ...]
cat_count: RDD = terms_cat.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)  # type: ignore
print(f"progress - cat_count: {cat_count.take(1)}")

# [(term, count), ...]
term_count: RDD = terms_cat.flatMap(lambda x: [(t, 1) for t in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore
print(f"progress - term_count: {term_count.take(1)}")

# [((term, cat), count), ...]
term_cat_count: RDD = terms_cat.flatMap(lambda x: [((t, x[1]), 1) for t in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore
print(f"progress - term_cat_count: {term_cat_count.take(1)}")


# def get_chi2(term: str, cat: str) -> float:
#     A: int = term_cat_count.lookup((term, cat))[0] if term_cat_count.lookup((term, cat)) else -1
#     B: int = term_count.lookup(term)[0] if term_count.lookup(term) else -1
#     C: int = cat_count.lookup(cat)[0] if cat_count.lookup(cat) else -1
#     D: int = N - A - B - C
#     assert A != -1 and B != -1 and C != -1 and D != -1
#     return N * (A * D - B * C) ** 2 / ((A + C) * (B + D) * (A + B) * (C + D))


# [(cat, [(term, chi2), ...]), ...]
# fmt: off
# cat_term_chi2: RDD = term_cat_count.map(lambda x: (x[0][1], (x[0][0], x[1]))) \
#     .join(term_count) \
#     .map(lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1]))) \
#     .join(cat_count) \
#     .map(lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][0][2], x[1][1]))) \
#     .map(lambda x: (x[1][0], (x[0], x[1][1], x[1][2], x[1][3], N - x[1][1] - x[1][2] - x[1][3]))) \
#     .map(lambda x: (x[0], N * (x[1][1] * x[1][4] - x[1][2] * x[1][3]) ** 2 / ((x[1][1] + x[1][3]) * (x[1][2] + x[1][4]) * (x[1][1] + x[1][2]) * (x[1][3] + x[1][4])))) \
#     .groupByKey() \
#     .mapValues(list)
# fmt: on
print()
# print(f"progress - cat_term_chi2: {cat_term_chi2.take(1)}")
