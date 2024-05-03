# pyspark only works with python 3.11 and java <21
# use docker or language runtime version manager.
# 
# python: 3.11
# 
# -   you aren't allowed to use the latest python12 with pyspark
# -   see: https://stackoverflow.com/a/77318636/13045051
# -   also `pyspark` and check if the workers use the same version of python as the driver
# 
# java: 11
# 
# -   pyspark breaks on java 21

brew install apache-spark

python3 -m pip install pyspark 'pyspark[sql]' 'pyspark[ml]' 'pyspark[streaming]'
