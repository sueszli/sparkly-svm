from pyspark import SparkContext, SparkConf
from pyspark.rdd import RDD

import pathlib
import json
import re


DATA_PATH = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
STOPWORD_PATH = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
OUTPUT_PATH = "output_rdd.txt"

conf = SparkConf().setAppName("chi2").setMaster("local[*]")
sc = SparkContext(conf=conf)


"""
tokenization, case folding, stopword removal
"""
# fmt: off
stopwords = sc.textFile(str(STOPWORD_PATH)).collect()
regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
get_terms = lambda text: [t for t in re.split(regex, text.lower()) if t and t not in stopwords]
terms_cat: RDD = sc.textFile(str(DATA_PATH)) \
                .map(json.loads) \
                .map(lambda jsn: (jsn["reviewText"], jsn["category"])) \
                .map(lambda tc: (get_terms(tc[0]), tc[1]))  # type: ignore
terms_cat.persist()
# fmt: on


"""
precompute
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


"""
chi2 computation
"""
# [(cat, [(term, chi2), ...]), ...]
# fmt: off
N: int = sc.textFile(str(DATA_PATH)).count()
get_chi2 = lambda n11, n10, n01, n00: N * (n11 * n00 - n10 * n01) ** 2 / ((n11 + n10) * (n01 + n00) * (n11 + n01) * (n10 + n00))
cat_term_chi2s_top75: RDD = term_cat_count \
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
