# rdd programming: https://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds

from pyspark import SparkContext, SparkConf
from pyspark.rdd import RDD
import pathlib

import json
import re
import time


conf = SparkConf().setAppName("chi2").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
datapath = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"

stoppath = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
stopwords = sc.textFile(str(stoppath)).collect()
regex = r"[ \t\d()\[\]{}.!?,;:+=\-_\"\'~#@&*%€$§\/]+"


# tokenization, case folding, stopword removal
# fmt: off
stime = time.perf_counter()
terms_cat: RDD = sc.textFile(str(datapath)) \
                .map(json.loads) \
                .map(lambda x: (x["reviewText"], x["category"])) \
                .map(lambda x: ([t for t in re.split(regex, x[0].lower()) if t and t not in stopwords], x[1]))
# fmt: on
print(f"terms_cat in {time.perf_counter() - stime:.4f}s -> {terms_cat.take(1)}")


# total num
stime = time.perf_counter()
N: int = sc.textFile(str(datapath)).count()
print(f"N in {time.perf_counter() - stime:.4f}s -> {N}")


# [(cat, count), ...]
stime = time.perf_counter()
cat_count: RDD = terms_cat.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)  # type: ignore
get_cat_count = lambda cat: cat_count.filter(lambda x: x[0] == cat).map(lambda x: x[1]).first()
print(f"cat_count in {time.perf_counter() - stime:.4f}s -> {cat_count.take(1)}")

# [(term, count), ...]
stime = time.perf_counter()
term_count: RDD = terms_cat.flatMap(lambda x: [(t, 1) for t in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore
get_term_count = lambda term: term_count.filter(lambda x: x[0] == term).map(lambda x: x[1]).first()
print(f"term_count in {time.perf_counter() - stime:.4f}s -> {term_count.take(1)}")


# [(term, cat, count), ...]
stime = time.perf_counter()
term_cat_count: RDD = terms_cat.flatMap(lambda x: [((t, x[1]), 1) for t in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore
get_term_cat_count = lambda term, cat: term_cat_count.filter(lambda x: x[0] == (term, cat)).map(lambda x: x[1]).first()
print(f"term_cat_count in {time.perf_counter() - stime:.4f}s -> {term_cat_count.take(1)}")


# probably faster to compute each of terms above in one go as it takes a while to start up the spark context


# def get_cat_term_chi(term: str, cat: str) -> tuple[str, str, float]:
#     tc11 = term_cat_count.filter(lambda x: x[0] == term and x[1] == cat).map(lambda x: x[2]).first()
#     tc10 = term_count.filter(lambda x: x[0] == term).map(lambda x: x[1]).first() - tc11
#     tc01 = cat_count.filter(lambda x: x[0] == cat).map(lambda x: x[1]).first() - tc11
#     tc00 = N - tc11 - tc10 - tc01
#     print(tc11, tc10, tc01, tc00)
#     return term, cat, (N * (tc11 * tc00 - tc10 * tc01) ** 2) / ((tc11 + tc10) * (tc11 + tc01) * (tc10 + tc00) * (tc01 + tc00))


# cat_term_chi = term_cat_count.map(lambda x: get_cat_term_chi(x[0][0], x[0][1]))

sc.stop()
