FROM jupyter/pyspark-notebook

USER root

# install additional python packages
RUN pip install pyspark findspark 'pyspark[sql]' 'pyspark[ml]' 'pyspark[streaming]'
RUN pip install numpy pandas matplotlib seaborn scikit-learn

# expose the ports for jupyter notebook, spark ui and history server
EXPOSE 8888 4040 4041

# mount the current directory to the container, set as working directory
# this is the default home directory given by the jupyter/pyspark-notebook image
VOLUME ["/home/jovyan/work"]
WORKDIR /home/jovyan/work

# allow notebook access without token
ENV JUPYTER_ENABLE_LAB=yes
CMD ["start-notebook.sh",
  "--ip=0.0.0.0", "--no-browser", "--allow-root",
  "--ServerApp.token=''", "--ServerApp.password=''",
  "--ServerApp.allow_origin='*'", "--ServerApp.disable_check_xsrf=True"]
