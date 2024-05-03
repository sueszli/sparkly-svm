from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import StopWordsRemover


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
# fmt: off
regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
df = RegexTokenizer(inputCol="reviewText", outputCol="terms", pattern=regex) \
    .transform(spark.read.json(str(DATA_PATH))) \
    .select("terms", "category")

stopwords = sc.textFile(str(STOPWORD_PATH)).collect()
df = StopWordsRemover(inputCol="terms", outputCol="filtered", stopWords=stopwords) \
    .transform(df) \
    .select("filtered", "category") \
    .withColumnRenamed("filtered", "terms")
# fmt: on

print(df.show())
