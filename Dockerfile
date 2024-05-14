FROM jupyter/pyspark-notebook

USER root

RUN pip install pyspark findspark 'pyspark[sql]' 'pyspark[ml]' 'pyspark[streaming]'
RUN pip install numpy pandas matplotlib seaborn scikit-learn

EXPOSE 8888 4040 4041

VOLUME ["/home/jovyan/work"]
WORKDIR /home/jovyan/work

ENV JUPYTER_ENABLE_LAB=yes
CMD ["start-notebook.sh", "--ip=0.0.0.0", "--no-browser", "--allow-root", "--ServerApp.token=''", "--ServerApp.password=''", "--ServerApp.allow_origin='*'", "--ServerApp.disable_check_xsrf=True"]
