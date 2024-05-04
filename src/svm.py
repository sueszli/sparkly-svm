from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer, ChiSqSelector, CountVectorizer, Normalizer
import pathlib


DATA_PATH = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
STOPWORD_PATH = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"

conf = SparkConf().setAppName("chi2").setMaster("local[*]")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
stopwords = sc.textFile(str(STOPWORD_PATH)).collect()
stages = [
    # get features
    RegexTokenizer(inputCol="reviewText", outputCol="rawTerms", pattern=regex),
    StopWordsRemover(inputCol="rawTerms", outputCol="terms", stopWords=stopwords),
    HashingTF(inputCol="terms", outputCol="rawFeatures"),
    IDF(inputCol="rawFeatures", outputCol="features"),
    StringIndexer(inputCol="category", outputCol="label"),
    ChiSqSelector(featuresCol="features", outputCol="selectedFeatures", labelCol="label", numTopFeatures=2000),

    # l2-normalize vector length
    Normalizer(inputCol="selectedFeatures", outputCol="normalizedFeatures")
]

pipeline = Pipeline(stages=stages)
df = spark.read.json(str(DATA_PATH)).select("reviewText", "category")
result = pipeline.fit(df).transform(df)
result.show()


"""
Train a Support Vector Machine text classifier model that can predict the product category from a review's text.

We're dealing with multi-class problems, so make sure to put a strategy in place that allows binary classifiers to be applicable.

Split the review data into training, validation, and test set.

Use a grid search for parameter optimization:

- Compare chi square overall top 2000 filtered features with another, heavier filtering with much less dimensionality (see Spark ML documentation for options).
- Compare different SVM settings by varying the regularization parameter (choose 3 different values), standardization of training features (2 values), and maximum number of iterations (2 values).

Use the MulticlassClassificationEvaluator to estimate performance of your trained classifiers on the test set, using F1 measure as criterion.
"""

train, val, test = result.randomSplit([0.7, 0.15, 0.15])
