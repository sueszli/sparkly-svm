see: https://tuwel.tuwien.ac.at/pluginfile.php/3988544/mod_page/content/1/Exercise%202.html

-   pyspark for text processing
-   use vpn to log into server: https://jupyter01.lbd.hpc.tuwien.ac.at/

part 1: ✅

-   see: https://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds
-   same as the previous assignment but this time with RDDs and transformations
-   output to `output_rdd.txt`, compare with the output of the previous assignment

part 2: ✅

-   see: https://spark.apache.org/docs/latest/ml-pipeline.html
-   use Spark MLlib (not the RDD version) to build a text classification pipeline
-   read in reviews, tokenize, remove stopwords, calculate the TF-IDF and chi-squared values for each word in the reviews
-   convert the reviews to a classic vector space representation with TFIDF-weighted features
-   pick top 2000 words with the highest chi-squared values
-   output to `output_ds.txt`, compare with the output of the previous assignment

part 3: ✅

see relevant slides: https://tuwel.tuwien.ac.at/pluginfile.php/3987338/mod_resource/content/1/Lecture5_%20Spark_SparkML.pdf

-   Train a Support Vector Machine text classifier from the features extracted from the first pipeline.
-   The goal is to learn a model that can predict the product category from a review's text.
-   Since we are dealing with multi-class problems, make sure to put a strategy in place that allows binary classifiers to be applicable.
-   Apply vector length normalization before feeding the feature vectors into the classifier (use Normalizer with L2 norm).
-   Make sure to:
    -   Split the review data into training, validation, and test set.
    -   Make experiments reproducible.
    -   Use a grid search for parameter optimization:
        -   Compare chi square overall top 2000 filtered features with another, heavier filtering with much less dimensionality (see Spark ML documentation for options).
        -   Compare different SVM settings by varying the regularization parameter (choose 3 different values), standardization of training features (2 values), and maximum number of iterations (2 values).
    -   Use the MulticlassClassificationEvaluator to estimate performance of your trained classifiers on the test set, using F1 measure as criterion.
