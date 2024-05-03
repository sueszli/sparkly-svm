# fmt: off

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF, ChiSqSelector, StringIndexer
from pyspark.ml.linalg import Vectors
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
tf-idf vectorization
"""
hashingTF = HashingTF(inputCol="terms", outputCol="rawFeatures")
tf = hashingTF.transform(df)

df = IDF(inputCol="rawFeatures", outputCol="features") \
    .fit(tf) \
    .transform(tf) \
    .select("terms", "features", "category")


"""
top 2000 chi2 features
"""
df = StringIndexer(inputCol="category", outputCol="label") \
    .fit(df) \
    .transform(df)

df = ChiSqSelector(numTopFeatures=2000, featuresCol="features", outputCol="selectedFeatures", labelCol="label") \
    .fit(df) \
    .transform(df) \
    .select("terms", "selectedFeatures", "category")

# group by category
df = df.groupBy("category") \
    .agg({"selectedFeatures": "collect_list", "terms": "collect_list"}) \
    .withColumnRenamed("collect_list(selectedFeatures)", "selectedFeatures") \
    .withColumnRenamed("collect_list(terms)", "terms")

# sort by category
df = df.sort("category")
