from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF, ChiSqSelector, StringIndexer
from pyspark.ml import Pipeline

from pyspark.ml.classification import LinearSVC
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.feature import Normalizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import pathlib


DATA_PATH = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
STOPWORD_PATH = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"

conf = SparkConf().setAppName("svm").setMaster("local[*]")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
stopwords = sc.textFile(str(STOPWORD_PATH)).collect()
stages = [
    RegexTokenizer(inputCol="reviewText", outputCol="terms", pattern=regex),
    StopWordsRemover(inputCol="terms", outputCol="filtered", stopWords=stopwords),
    HashingTF(inputCol="filtered", outputCol="rawFeatures"),
    IDF(inputCol="rawFeatures", outputCol="features"),
    StringIndexer(inputCol="category", outputCol="label"),
    ChiSqSelector(featuresCol="features", outputCol="selectedFeatures", labelCol="label", numTopFeatures=2000)
]

pipeline = Pipeline(stages=stages)
df = spark.read.json(str(DATA_PATH)).select("reviewText", "category")
model = pipeline.fit(df)
result_df = model.transform(df)
result_df = result_df.select("category", "label", "selectedFeatures")

result_df.show()
