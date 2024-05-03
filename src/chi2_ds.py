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
Convert the review texts to a classic vector space representation with TFIDF-weighted features (using 2000 top terms overall)

- HashingTF, IDF, ChiSqSelector
"""
hashingTF = HashingTF(inputCol="terms", outputCol="rawFeatures", numFeatures=2000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
chi2 = ChiSqSelector(numTopFeatures=2000, featuresCol="features", outputCol="selectedFeatures")
df = hashingTF.transform(df)
df = idf.fit(df).transform(df)
df = chi2.fit(df).transform(df)

print(df.show())
