from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF, ChiSqSelector, StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

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
    
    # convert the terms into hashed term frequencies
    HashingTF(inputCol="terms", outputCol="rawFeatures"),

    # calculate the TF-IDF weights for each term
    IDF(inputCol="rawFeatures", outputCol="features"),

    # encode 'category' as a numeric label (unique to the category in given row)
    StringIndexer(inputCol="category", outputCol="label"),

    # get chi-squared score of each term (top 2000 overall)
    ChiSqSelector(featuresCol="features", outputCol="selectedFeatures", labelCol="label", numTopFeatures=2000)
]

pipeline = Pipeline(stages=stages)
df = spark.read.json(str(DATA_PATH)).select("reviewText", "category")
model = pipeline.fit(df)
result_df = model.transform(df)
# result_df = result_df.select("category", "label", "terms", "selectedFeatures")


# TODO: get column with original terms from the chi-squared selected features




for row in result_df.collect():
    print(row["terms"], row["rawFeatures"], row["features"], row["selectedFeatures"])
    print("\n\n\n")
