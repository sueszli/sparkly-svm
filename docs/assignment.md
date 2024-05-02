see: https://tuwel.tuwien.ac.at/pluginfile.php/3988544/mod_page/content/1/Exercise%202.html

-   pyspark for text processing
-   use vpn to log into server: https://jupyter01.lbd.hpc.tuwien.ac.at/

part 1: ✅

-   see: https://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds
-   same as the previous assignment but this time with RDDs and transformations
-   output to `output_rdd.txt`, compare with the output of the previous assignment

part 2:

-   see: https://spark.apache.org/docs/latest/ml-pipeline.html
-   use Spark MLlib (not the RDD version) to build a text classification pipeline
-   read in reviews, tokenize, remove stopwords, calculate the TF-IDF and chi-squared values for each word in the reviews
-   convert the reviews to a classic vector space representation with TFIDF-weighted features
-   pick top 2000 words with the highest chi-squared values
-   output to `output_ds.txt`, compare with the output of the previous assignment

part 3:

-   train a SVM as a text classifier on the vector representations to predict the category of the review
