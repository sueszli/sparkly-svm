# install java 11 (or switch, using sdkman: https://sdkman.io/)
brew install openjdk@11
java --version

# install spark
brew install apache-spark
pyspark --version

# pyarrow only works up to python 3.11 (as of 2024-05-01) -> see: https://stackoverflow.com/a/77318636/13045051
# it doesn't matter which version of python you have installed, as long as it matches with the workers of the spark cluster (check by running `pyspark` and reading the first few lines of the logs)
# it's most likely the default python version of your system (e.g. python 3.7)
brew install python@3.11
python3.11 --version

# freeze requirements
python3.11 -m pip install --upgrade pip > /dev/null
python3.11 -m pip install pipreqs > /dev/null
pipreqs .

# install pyspark
python3.11 -m pip install pyspark 'pyspark[sql]' 'pyspark[ml]' 'pyspark[streaming]'
