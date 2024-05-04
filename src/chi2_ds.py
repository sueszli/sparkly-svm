# fmt: off
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, IDF, StringIndexer, ChiSqSelector, CountVectorizer, CountVectorizerModel
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
    RegexTokenizer(inputCol="reviewText", outputCol="rawTerms", pattern=regex),
    StopWordsRemover(inputCol="rawTerms", outputCol="terms", stopWords=stopwords),

    CountVectorizer(inputCol="terms", outputCol="rawFeatures"),
    IDF(inputCol="rawFeatures", outputCol="features"),

    StringIndexer(inputCol="category", outputCol="label"),
    ChiSqSelector(featuresCol="features", outputCol="selectedFeatures", labelCol="label", numTopFeatures=2000),
]

pipeline = Pipeline(stages=stages)
df = spark.read.json(str(DATA_PATH)).select("reviewText", "category")
model = pipeline.fit(df)
result = model.transform(df)
result.show()

# get all selected features through chisq index from vocabulary
cv_model: CountVectorizerModel = model.stages[2] # type: ignore
chisq_model: ChiSqSelector = model.stages[5] # type: ignore

cv_vocab: list[str] = cv_model.vocabulary
chisq_idx: list[int] = chisq_model.selectedFeatures # type: ignore

selection: list[str] = sorted(set([cv_vocab[i] for i in chisq_idx]))

print(f"{len(selection)=}, {len(cv_vocab)=}, {len(chisq_idx)=}")
with open(OUTPUT_PATH, "w") as f:
    f.write("\n".join(selection))
