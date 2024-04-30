if ! command -v java &> /dev/null; then echo "java missing"; exit 1; fi
if ! command -v python3 &> /dev/null; then echo "python3 missing"; exit 1; fi
if ! command -v pip &> /dev/null; then echo "pip missing"; exit 1; fi

# pyarrow only works up to python 3.11 (as of 2024-05-01)
# see: https://stackoverflow.com/a/77318636/13045051

brew install python@3.11

# ---

python3.11 -m pip install --upgrade pip > /dev/null

python3.11 -m pip install black > /dev/null
python3.11 -m pip install pipreqs > /dev/null

rm -rf requirements.txt > /dev/null
pipreqs . > /dev/null
python3.11 -m pip install -r requirements.txt > /dev/null

# ---

python3.11 -m pip install pyspark 'pyspark[sql]' 'pyspark[ml]' 'pyspark[streaming]'
