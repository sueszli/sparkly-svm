from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
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
    RegexTokenizer(inputCol="reviewText", outputCol="rawTerms", pattern=regex),
    StopWordsRemover(inputCol="rawTerms", outputCol="terms", stopWords=stopwords),
    
    # calculate the TF-IDF weights for each term

    # get chi-squared score of each term (top 2000 overall)

    # get the original terms from the selected features
]

pipeline = Pipeline(stages=stages)
df = spark.read.json(str(DATA_PATH)).select("reviewText", "category")
result = pipeline.fit(df).transform(df)
result.show()
