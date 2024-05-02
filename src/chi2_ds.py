from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


import pathlib

DATA_PATH = pathlib.Path(__file__).parent.parent / "data" / "reviews_devset.json"
STOPWORD_PATH = pathlib.Path(__file__).parent.parent / "data" / "stopwords.txt"
OUTPUT_PATH = "output_ds.txt"

conf = SparkConf().setAppName("chi2").setMaster("local[*]")
sc = SparkContext(conf=conf)

# read data as DataFrame

spark = SparkSession(sc)
df = spark.read.json(str(DATA_PATH))
df.createOrReplaceTempView("reviews")

print(df.show())
