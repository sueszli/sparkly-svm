from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, IDF, StringIndexer, ChiSqSelector, CountVectorizer, CountVectorizerModel, Normalizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import pandas as pd
import numpy as np
from pyspark.sql.functions import col
from sklearn.model_selection import train_test_split

import pathlib


"""
Train a Support Vector Machine text classifier model that can predict the product category from a review's text.

We're dealing with multi-class problems, so make sure to put a strategy in place that allows binary classifiers to be applicable.

Split the review data into training, validation, and test set.

Use a grid search for parameter optimization:

- Compare chi square overall top 2000 filtered features with another, heavier filtering with much less dimensionality (see Spark ML documentation for options).
- Compare different SVM settings by varying the regularization parameter (choose 3 different values), standardization of training features (2 values), and maximum number of iterations (2 values).

Use the MulticlassClassificationEvaluator to estimate performance of your trained classifiers on the test set, using F1 measure as criterion.
"""


DATA_PATH = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
STOPWORD_PATH = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"

conf = SparkConf().setAppName("svm").setMaster("local[*]")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
stopwords = sc.textFile(str(STOPWORD_PATH)).collect()
pipeline = Pipeline(stages=[
    # get features
    RegexTokenizer(inputCol="reviewText", outputCol="rawTerms", pattern=regex),
    StopWordsRemover(inputCol="rawTerms", outputCol="terms", stopWords=stopwords),

    CountVectorizer(inputCol="terms", outputCol="rawFeatures"),
    IDF(inputCol="rawFeatures", outputCol="features"),

    StringIndexer(inputCol="category", outputCol="label"),
    ChiSqSelector(featuresCol="features", outputCol="selectedFeatures", labelCol="label", numTopFeatures=2000),

    # l2-normalize vector length
    Normalizer(inputCol="selectedFeatures", outputCol="normalizedFeatures"),

    # train SVM
    OneVsRest(classifier=LinearSVC(featuresCol="normalizedFeatures", labelCol="label"))
])

# split data
df = spark.read.json(str(DATA_PATH)).select("reviewText", "category")
train, val, test = df.randomSplit([0.6, 0.2, 0.2], seed=42)

# define search grid (make it super small for faster execution)
param_grid = ParamGridBuilder() \
    .addGrid(ChiSqSelector.numTopFeatures, [100]) \
    .addGrid(LinearSVC.regParam, [0.1]) \
    .addGrid(LinearSVC.maxIter, [100]) \
    .build()

# define cross validator
cv = CrossValidator(estimator=pipeline, 
                    estimatorParamMaps=param_grid,
                    evaluator=MulticlassClassificationEvaluator(metricName="f1"),
                    numFolds=3)

# train model
cv_model = cv.fit(train)

# make predictions on test set
predictions = cv_model.transform(test)

# evaluate
evaluator = MulticlassClassificationEvaluator(metricName="f1")
f1 = evaluator.evaluate(predictions)
print(f"F1 score: {f1}")
