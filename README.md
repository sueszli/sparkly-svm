## running in container

first run the provided shell script.

then either work inside the container or use a jupyter notebook via the exposed port.

nice tutorial on how to work in a container:

-   https://www.youtube.com/watch?v=0H2miBK_gAk
-   https://github.com/patrickloeber/python-docker-tutorial

used image:

-   https://hub.docker.com/r/jupyter/pyspark-notebook
-   https://github.com/jupyter/docker-stacks/blob/main/images/pyspark-notebook/Dockerfile
-   https://jupyter-docker-stacks.readthedocs.io/en/latest/using/specifics.html

## running locally

pyspark doesn't work with the latest 3.12 python and java 21 versions (as of may 2024) because of an incompatibility issue with `pyarrow`.

see: https://stackoverflow.com/a/77318636/13045051

-   the jupyter team is using: python@3.11 and java@17.
-   use a language runtime version manager like `asdf`.
-   make sure that the workers use the same version of python as the driver. you can check this by running `pyspark` and reading the logs.

then install the following packages:

```bash
brew install apache-spark

python3 -m pip install pyspark 'pyspark[sql]' 'pyspark[ml]' 'pyspark[streaming]'
```
