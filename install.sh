# install java 11
brew install openjdk@11
java --version

# alternatively: change the java version using sdkman
sdk install java 11.0.21-amzn
sdk use java 11.0.21-amzn
java --version

# install spark
brew install apache-spark
brew info apache-spark
pyspark --version

# pyarrow only works up to python 3.11 (as of 2024-05-01) -> see: https://stackoverflow.com/a/77318636/13045051
# it doesn't matter which version of python you have installed, as long as it matches with the workers of the spark cluster (check by running `pyspark` and reading the first few lines of the logs).

# install python 3.11
brew install python@3.11
python3.11 --version

# install pyspark
python3.11 -m pip install pyspark 'pyspark[sql]' 'pyspark[ml]' 'pyspark[streaming]'
