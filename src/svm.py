# fmt: off
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, IDF, StringIndexer, ChiSqSelector, CountVectorizer, CountVectorizerModel, Normalizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, TrainValidationSplit
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import pandas as pd
import numpy as np
from pyspark.sql.functions import col
from sklearn.model_selection import train_test_split

import pathlib

import argparse


"""
Train a Support Vector Machine text classifier model that can predict the product category from a review's text.

We're dealing with multi-class problems, so make sure to put a strategy in place that allows binary classifiers to be applicable.

Split the review data into training, validation, and test set.

Use a grid search for parameter optimization:

- Compare chi square overall top 2000 filtered features with another, heavier filtering with much less dimensionality (see Spark ML documentation for options).
- Compare different SVM settings by varying the regularization parameter (choose 3 different values), standardization of training features (2 values), and maximum number of iterations (2 values).

Use the MulticlassClassificationEvaluator to estimate performance of your trained classifiers on the test set, using F1 measure as criterion.
"""

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cross-val", action='store_true')
    return parser.parse_args()

args = parse_args()

DATA_PATH = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
STOPWORD_PATH = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"

conf = SparkConf() \
    .setAppName("svm") \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "4g") \
    .set("spark.driver.maxResultSize", "2g") \
    .set("spark.default.parallelism", "12")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

regex = r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+'
stopwords = sc.textFile(str(STOPWORD_PATH)).collect()
preprocessing_pipeline = Pipeline(stages=[
    RegexTokenizer(inputCol="reviewText", outputCol="rawTerms", pattern=regex),
    StopWordsRemover(inputCol="rawTerms", outputCol="terms", stopWords=stopwords),
    CountVectorizer(inputCol="terms", outputCol="rawFeatures"),
    IDF(inputCol="rawFeatures", outputCol="features"),
    StringIndexer(inputCol="category", outputCol="label"),   
])

estimator_pipeline = Pipeline(stages=[
    ChiSqSelector(numTopFeatures=2000, featuresCol="features", outputCol="selectedFeatures", labelCol="label"),
    Normalizer(inputCol="selectedFeatures", outputCol="normalizedFeatures"),
    OneVsRest(classifier=LinearSVC(featuresCol="normalizedFeatures", labelCol="label"))
])


# split data
df = spark.read.json(str(DATA_PATH)).select("reviewText", "category")
df = preprocessing_pipeline.fit(df).transform(df)
train, test = df.randomSplit([0.8, 0.2], seed=42)

# define search grid (make it super small for faster execution)
param_grid = ParamGridBuilder() \
    .addGrid(ChiSqSelector.numTopFeatures, [10]) \
    .addGrid(LinearSVC.regParam, [0.1]) \
    .addGrid(LinearSVC.maxIter, [10]) \
    .build()

# define evaluator
evaluator = MulticlassClassificationEvaluator(metricName="f1")

# define cross validator
if args.cross_val:
    cv = CrossValidator(estimator=estimator_pipeline, 
                        estimatorParamMaps=param_grid,
                        evaluator=evaluator,
                        parallelism=10,
                        numFolds=3,
                        seed=42)
else:
    cv = TrainValidationSplit(estimator=estimator_pipeline, 
                    estimatorParamMaps=param_grid,
                    evaluator=evaluator,
                    trainRatio=0.8,
                    parallelism=10,
                    seed=42)

# train model
cv_model = cv.fit(train)

# make predictions on test set
predictions = cv_model.transform(test)

# evaluate
f1 = evaluator.evaluate(predictions)
print(f"f1 score: {f1}")
