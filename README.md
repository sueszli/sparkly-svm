# running in container

run the installation script.

nice tutorial: https://www.youtube.com/watch?app=desktop&v=0H2miBK_gAk

based on:

-   https://hub.docker.com/r/jupyter/pyspark-notebook
-   https://github.com/jupyter/docker-stacks/blob/main/images/pyspark-notebook/Dockerfile
-   https://jupyter-docker-stacks.readthedocs.io/en/latest/using/specifics.html

# running locally

pyspark doesn't work with the latest 3.12 python and java 21 (as of may 2024) because of an incompatibility issue with `pyarrow`.

see: https://stackoverflow.com/a/77318636/13045051

-   the jupyter team is using: python@3.11 and java@17.
-   use a language runtime version manager like `asdf`.
-   make sure that the workers use the same version of python as the driver. you can make sure by running `pyspark` to open the spark shell and reading the first log lines that show the python version.

once that is done, you can just install spark and pyspark like so:

```bash
brew install apache-spark

python3 -m pip install pyspark 'pyspark[sql]' 'pyspark[ml]' 'pyspark[streaming]'
```
