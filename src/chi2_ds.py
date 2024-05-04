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
result_df.show()

# print the terms selected by the ChiSqSelector
for row in result_df.collect():
    selected_features_dict = dict(zip(row["selectedFeatures"].indices, row["selectedFeatures"].values))
    selected_features_dict = {k: v for k, v in sorted(selected_features_dict.items(), key=lambda item: item[1], reverse=True)}

    print(f"category: {row['category']}")
    print(f"rawTerms: {row['rawTerms']}")
    print(f"terms: {row['terms']}")

    print(f"lengths: rawTerms {len(row['rawTerms'])}, terms {len(row['terms'])}, rawFeatures {len(row['rawFeatures'].values)}, features {len(row['features'].values)}, selectedFeatures {len(selected_features_dict)}")

    for term_idx, score in selected_features_dict.items():
        print(f"\t{term_idx}: {score}")

    print("\n\n")
