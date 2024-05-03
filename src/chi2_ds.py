# fmt: off

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF, ChiSqSelector, StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

import pathlib


DATA_PATH = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
STOPWORD_PATH = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
OUTPUT_PATH = "output_ds.txt"

conf = SparkConf().setAppName("chi2").setMaster("local[*]")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


"""
tokenization, case folding, stopword removal
"""
regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
df = RegexTokenizer(inputCol="reviewText", outputCol="terms", pattern=regex) \
    .transform(spark.read.json(str(DATA_PATH))) \
    .select("terms", "category")

stopwords = sc.textFile(str(STOPWORD_PATH)).collect()
df = StopWordsRemover(inputCol="terms", outputCol="filtered", stopWords=stopwords) \
    .transform(df) \
    .select("filtered", "category") \
    .withColumnRenamed("filtered", "terms")


"""
convert the review texts to a classic vector space representation
with TFIDF-weighted features (using 2000 top terms overall)
"""
# compute {term hash: term idf} for each review
tf = HashingTF(inputCol="terms", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")
df = idf.fit(tf.transform(df)).transform(tf.transform(df))

# encode category to numeric label
indexer = StringIndexer(inputCol="category", outputCol="label")
df = indexer.fit(df).transform(df)

# compute top 2000 highest chi-squared features
selector = ChiSqSelector(numTopFeatures=2000, featuresCol="features", outputCol="selectedFeatures", labelCol="label")
df = selector.fit(df).transform(df)

# 

print(df.show())
