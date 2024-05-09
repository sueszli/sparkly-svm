# fmt: off
from pyspark import SparkContext, SparkConf
from pyspark.rdd import RDD

import pathlib
import json
import re


DATA_PATH = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
STOPWORD_PATH = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
OUTPUT_PATH = pathlib.Path(__file__).parent.parent / "data" /  "output_rdd.txt"

conf = SparkConf().setAppName("chi2").setMaster("local[*]")
sc = SparkContext(conf=conf)


"""
tokenization, case folding, stopword removal
"""
stopwords = sc.textFile(str(STOPWORD_PATH)).collect()
regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
get_terms = lambda text: list(set([t for t in re.split(regex, text.lower()) if t and t not in stopwords and len(t) > 1]))
terms_cat: RDD = sc.textFile(str(DATA_PATH)) \
                .map(json.loads) \
                .map(lambda jsn: (jsn["reviewText"], jsn["category"])) \
                .map(lambda tc: (get_terms(tc[0]), tc[1]))  # type: ignore
terms_cat.persist()


"""
chi2 calculation
"""
# [(cat, count), ...]
cat_count: RDD = terms_cat.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)  # type: ignore
cat_count_broadcast = sc.broadcast(dict(cat_count.collect()))
get_cat_count = lambda cat: cat_count_broadcast.value[cat]

# [(term, count), ...]
term_count: RDD = terms_cat.flatMap(lambda x: [(t, 1) for t in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore
term_count_broadcast = sc.broadcast(dict(term_count.collect()))
get_term_count = lambda term: term_count_broadcast.value[term]

# [((term, cat), count), ...]
term_cat_count: RDD = terms_cat.flatMap(lambda x: [((t, x[1]), 1) for t in x[0]]).reduceByKey(lambda a, b: a + b)  # type: ignore
term_cat_count_broadcast = sc.broadcast(dict(term_cat_count.collect()))
get_term_cat_count = lambda term, cat: term_cat_count_broadcast.value[(term, cat)]

# [(cat, [(term, chi2), ...]), ...]
# N: int = sc.textFile(str(DATA_PATH)).count() --> reading the file again is way more expensive
N: int = sum(cat_count_broadcast.value.values())
get_chi2 = lambda A, B, C, D: N * (A * D - B * C) ** 2 / ((A + B) * (C + D) * (A + C) * (B + D))
cat_term_chi2s_top75: RDD = term_cat_count \
    .map(lambda tc_c: (
        tc_c[0][1], # cat
        (
            tc_c[0][0], # term
            get_chi2(
                A=tc_c[1],
                B=get_term_count(tc_c[0][0]) - tc_c[1],
                C=get_cat_count(tc_c[0][1]) - tc_c[1],
                D=N - tc_c[1] - get_term_count(tc_c[0][0]) - get_cat_count(tc_c[0][1])
            )
        )
    )) \
    .groupByKey() \
    .sortByKey() \
    .mapValues(lambda t_chi2s: sorted(t_chi2s, key=lambda x: x[1], reverse=True)[:75])
sorted_top75_terms = cat_term_chi2s_top75.flatMap(lambda x: [t[0] for t in x[1]]).distinct().sortBy(lambda x: x)


"""
save in output_rdd.txt
"""
with open(OUTPUT_PATH, "w") as f:
    for cat, term_chi2s in cat_term_chi2s_top75.collect():
        f.write(f"{cat} {' '.join([f'{term}:{chi2}' for term, chi2 in term_chi2s])}\n")
    f.write(" ".join(sorted_top75_terms.collect()))

terms_cat.unpersist()
sc.stop()
