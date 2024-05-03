# local development

pyspark doesn't work with the latest 3.12 python and java 21 (as of may 2024) because of an incompatibility issue with `pyarrow`.

see: https://stackoverflow.com/a/77318636/13045051

-   the jupyter team is using: python@3.11 and java@17
-   use a language runtime version manager like `asdf`

# pyspark only works with python 3.11 and java <21

# use docker or language runtime version manager.

#

# python: 3.11

#

# - you aren't allowed to use the latest python12 with pyspark

# - see: https://stackoverflow.com/a/77318636/13045051

# - also `pyspark` and check if the workers use the same version of python as the driver

#

# java: 11

#

# - pyspark breaks on java 21

---

# see:

# - https://hub.docker.com/r/jupyter/pyspark-notebook

# - https://github.com/jupyter/docker-stacks/blob/main/images/pyspark-notebook/Dockerfile

# - https://jupyter-docker-stacks.readthedocs.io/en/latest/using/specifics.html
