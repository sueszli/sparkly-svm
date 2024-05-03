# same as running:
# 
# - docker run -p 8888:8888 -p 4040:4040 -p 4041:4041 quay.io/jupyter/pyspark-notebook
# - docker run -p 8888:8888 -p 4040:4040 -p 4041:4041 -v $(pwd):/home/jovyan/work quay.io/jupyter/pyspark-notebook
# - open http://127.0.0.1:8888/

# run
docker-compose up

# use jupyter notebook in browser
# or attach vscode / pycharm to the container
open http://127.0.0.1:8888/

# stop and clean up
docker-compose down

# cleanup (optional)
ids=$(docker ps -a -q)
for id in $ids; do docker stop $id; done
for id in $ids; do docker rm $id; done
docker ps --all
