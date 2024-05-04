from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer, ChiSqSelector, CountVectorizer
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

    # category -> numerical label
    StringIndexer(inputCol="category", outputCol="label"),

    # get chi-squared score of each term (top 2000 overall)
    ChiSqSelector(featuresCol="features", outputCol="selectedFeatures", labelCol="label", numTopFeatures=2000)
]

pipeline = Pipeline(stages=stages)
df = spark.read.json(str(DATA_PATH)).select("reviewText", "category")
result = pipeline.fit(df).transform(df)
result.show()


"""
idk how to print the terms selected by the ChiSqSelector.
a transformation process (like hashing) inherently loses the direct mapping between the original terms and the selected features.
"""

for row in result.collect()[0:5]:
    selected_features_dict = dict(zip(row.selectedFeatures.indices, row.selectedFeatures.values))
    selected_features_dict = {k: v for k, v in sorted(selected_features_dict.items(), key=lambda item: item[1], reverse=True)}

    print(f"{len(row.category)=} - {len(row.rawTerms)=} - {len(row.terms)=} - {len(row.rawFeatures.values)=} - {len(row.features.values)=} - {len(selected_features_dict)=}")
    print(f"{row.category=}")
    print(f"{row.rawTerms=}")
    print(f"{row.terms=}")
    print(f"{row.rawFeatures.values=}")
    print(f"{row.features.values=}")
    print(f"{selected_features_dict=}")
    print("\n\n\n")
