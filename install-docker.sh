# same as running:
# 
# - docker run -p 8888:8888 -p 4040:4040 -p 4041:4041 quay.io/jupyter/pyspark-notebook
# - docker run -p 8888:8888 -p 4040:4040 -p 4041:4041 -v $(pwd):/home/jovyan/work quay.io/jupyter/pyspark-notebook
open http://127.0.0.1:8888/

# run
docker build -t sparkly-svm .
docker-compose up -d

# use jupyter notebook in browser or attach vscode / pycharm to the container
open http://127.0.0.1:8888/

# stop
docker-compose down

# cleanup
docker ps --all
docker stop <container_id>
docker rm <container_id>
