# fmt: off
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
pipeline = Pipeline(stages=[
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
    .addGrid(ChiSqSelector.numTopFeatures, [10]) \
    .addGrid(LinearSVC.regParam, [0.1]) \
    .addGrid(LinearSVC.maxIter, [10]) \
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
print(f"f1 score: {f1}")
