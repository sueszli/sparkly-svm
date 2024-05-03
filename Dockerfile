FROM jupyter/pyspark-notebook

USER root

# install additional python packages
RUN pip install pyspark findspark 'pyspark[sql]' 'pyspark[ml]' 'pyspark[streaming]'
RUN pip install numpy pandas matplotlib seaborn scikit-learn

# expose the ports for jupyter notebook, spark ui and history server
EXPOSE 8888 4040 4041

# mount the current directory to the container, set as working directory
VOLUME ["/home/jovyan/work"]
WORKDIR /home/jovyan/work

# allow notebook access without token
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
