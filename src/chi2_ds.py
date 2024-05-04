from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF, ChiSqSelector, StringIndexer
from pyspark.ml import Pipeline

import pathlib


DATA_PATH = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
STOPWORD_PATH = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
OUTPUT_PATH = pathlib.Path(__file__).parent.parent / "data" / "output_ds.txt"

conf = SparkConf().setAppName("chi2").setMaster("local[*]")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
stopwords = sc.textFile(str(STOPWORD_PATH)).collect()
stages = [
    # tokenization, stopword removal, case folding
    RegexTokenizer(inputCol="reviewText", outputCol="terms", pattern=regex),
    StopWordsRemover(inputCol="terms", outputCol="filtered", stopWords=stopwords),
    
    # convert the filtered terms into hashed term frequencies
    HashingTF(inputCol="filtered", outputCol="rawFeatures"),

    # calculate the TF-IDF weights for each term
    IDF(inputCol="rawFeatures", outputCol="features"),

    # encode 'category' as a numeric label (unique to the category in given row)
    StringIndexer(inputCol="category", outputCol="label"),

    # select the top 2000 features based on chi-squared test
    ChiSqSelector(featuresCol="features", outputCol="selectedFeatures", labelCol="label", numTopFeatures=2000)
]

pipeline = Pipeline(stages=stages)
df = spark.read.json(str(DATA_PATH)).select("reviewText", "category")
model = pipeline.fit(df)
result_df = model.transform(df)
