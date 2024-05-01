# rdd programming: https://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds

from pyspark import SparkContext, SparkConf
from pyspark.rdd import RDD
import pathlib

from typing import Tuple
import json
import re
import logging
import time


conf = SparkConf().setAppName("chi2").setMaster("local[*]")
sc = SparkContext(conf=conf)
datapath = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"


# tokenization, case folding, stopword removal
# fmt: off
stoppath = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
stopwords = sc.textFile(str(stoppath)).collect()
regex = r"[ \t\d()\[\]{}.!?,;:+=\-_\"\'~#@&*%€$§\/]+"
get_terms = lambda text: [t for t in re.split(regex, text.lower()) if t and t not in stopwords]
terms_cat: RDD = sc.textFile(str(datapath)) \
                .map(json.loads) \
                .map(lambda jsn: (jsn["reviewText"], jsn["category"])) \
                .map(lambda tc: (get_terms(tc[0]), tc[1]))  # type: ignore
# fmt: on
print(f"terms_cat -> {terms_cat.take(1)}")


# total num
N: int = sc.textFile(str(datapath)).count()
print(f"N -> {N}")


# [(cat, count), ...]
cat_count: RDD = terms_cat.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)  # type: ignore
cat_count_broadcast = sc.broadcast(dict(cat_count.collect()))
get_cat_count = lambda cat: cat_count_broadcast.value[cat]
print(f"cat_count -> {cat_count.take(1)}")


# [(term, count), ...]
term_count: RDD = terms_cat.flatMap(lambda x: [(t, 1) for t in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore
term_count_broadcast = sc.broadcast(dict(term_count.collect()))
get_term_count = lambda term: term_count_broadcast.value[term]
print(f"term_count -> {term_count.take(1)}")


# [((term, cat), count), ...]
term_cat_count: RDD = terms_cat.flatMap(lambda x: [((t, x[1]), 1) for t in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore
term_cat_count_broadcast = sc.broadcast(dict(term_cat_count.collect()))
get_term_cat_count = lambda term, cat: term_cat_count_broadcast.value[(term, cat)]
print(f"term_cat_count -> {term_cat_count.take(1)}")


# [(cat, [(term, chi2), ...]), ...]
# fmt: off
get_chi2 = lambda n11, n10, n01, n00: N * (n11 * n00 - n10 * n01) ** 2 / ((n11 + n10) * (n01 + n00) * (n11 + n01) * (n10 + n00))
cat_term_chi2s: RDD = term_cat_count \
    .map(lambda tc_c: (
        tc_c[0][1], # cat
        (
            tc_c[0][0], # term
            get_chi2(
                n11=tc_c[1],
                n10=get_term_count(tc_c[0][0]) - tc_c[1],
                n01=get_cat_count(tc_c[0][1]) - tc_c[1],
                n00=N - tc_c[1] - get_term_count(tc_c[0][0]) - get_cat_count(tc_c[0][1])
            )
        )
    )) \
    .groupByKey() \
    .sortByKey() \
    .mapValues(lambda t_chi2s: sorted(t_chi2s, key=lambda x: x[1], reverse=True)[:75])
# fmt: on
print(f"cat_term_chi2s -> {cat_term_chi2s.take(1)}")

# print all cat_term_chi2s
for cat, term_chi2s in cat_term_chi2s.collect():
    print(f"{cat} -> {list(term_chi2s)}")
    print("\n\n\n\n\n")
