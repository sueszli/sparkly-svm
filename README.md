![](./docs/Pipeline%20Ilustration.png)

<br><br>

![](./docs/f1_val_hyper.png)

![](./docs/f1_val_hyper2.png)

| numTopFeatures | regParam | maxIter |   p | f1_train |   f1_val | runtime_training | runtime_train_pred | runtime_train_val | runtime_train_eval |
| -------------: | -------: | ------: | --: | -------: | -------: | ---------------: | -----------------: | ----------------: | -----------------: |
|             10 |     0.01 |      10 |   1 | 0.872742 |  0.60184 |               90 |                  4 |                 3 |                 11 |
|             10 |     0.01 |      10 |   2 | 0.872742 |  0.60184 |               72 |                  4 |                 3 |                 10 |
|             10 |     0.01 |     100 |   1 | 0.938119 | 0.606172 |              551 |                  3 |                 3 |                 11 |
|             10 |     0.01 |     100 |   2 | 0.938183 | 0.606335 |              562 |                  3 |                 3 |                 11 |
|             10 |      0.1 |      10 |   1 | 0.871112 | 0.593461 |               74 |                  3 |                 3 |                 11 |
|             10 |      0.1 |      10 |   2 | 0.871112 | 0.593461 |               77 |                  4 |                 3 |                 11 |
|             10 |      0.1 |     100 |   1 | 0.908307 |   0.6031 |              588 |                  3 |                 3 |                 12 |
|             10 |      0.1 |     100 |   2 | 0.908307 |   0.6031 |              585 |                  3 |                 3 |                 12 |
|             10 |        1 |      10 |   1 | 0.782206 | 0.534759 |               73 |                  3 |                 3 |                 10 |
|             10 |        1 |      10 |   2 | 0.782206 | 0.534759 |               76 |                  3 |                 3 |                 10 |
|             10 |        1 |     100 |   1 | 0.850733 | 0.577915 |              582 |                  3 |                 3 |                 10 |
|             10 |        1 |     100 |   2 | 0.850733 | 0.577915 |              584 |                  3 |                 3 |                 12 |
|           2000 |     0.01 |      10 |   1 | 0.872742 |  0.60184 |               77 |                  3 |                 3 |                 11 |
|           2000 |     0.01 |      10 |   2 | 0.872742 |  0.60184 |               73 |                  3 |                 3 |                 11 |
|           2000 |     0.01 |     100 |   1 | 0.938123 | 0.606489 |              583 |                  3 |                 3 |                 11 |
|           2000 |     0.01 |     100 |   2 | 0.938183 | 0.606181 |              581 |                  3 |                 3 |                 11 |
|           2000 |      0.1 |      10 |   1 | 0.871112 | 0.593461 |               73 |                  3 |                 3 |                 10 |
|           2000 |      0.1 |      10 |   2 | 0.871112 | 0.593461 |               73 |                  3 |                 3 |                  9 |
|           2000 |      0.1 |     100 |   1 | 0.908307 |   0.6031 |              585 |                  3 |                 3 |                 13 |
|           2000 |      0.1 |     100 |   2 | 0.908307 |   0.6031 |              588 |                  3 |                 3 |                 10 |
|           2000 |        1 |      10 |   1 | 0.782206 | 0.534759 |               76 |                  3 |                 3 |                 12 |
|           2000 |        1 |      10 |   2 | 0.782206 | 0.534759 |               72 |                  3 |                 3 |                 10 |
|           2000 |        1 |     100 |   1 |  0.85071 | 0.577888 |              582 |                  4 |                 4 |                 11 |
|           2000 |        1 |     100 |   2 |  0.85071 | 0.577888 |              567 |                  4 |                 3 |                 11 |

<br><br>

# install

```
   ∧,,,∧
 (  ̳• · • ̳)   ⋆｡°✩ ⋆⁺｡˚⋆˙‧₊✩₊‧˙⋆˚｡⁺⋆ ✩°｡⋆ ・゜゜・． ｡･ﾟﾟ･　
/    づ✩₊˚.⋆☾⋆ °✩ sparkly support vector machine .・゜゜・　　
```

_docker_

-   run `docker-compose up` - see `./run.sh` for more details.

-   nice tutorial:

    -   https://www.youtube.com/watch?v=0H2miBK_gAk
    -   https://github.com/patrickloeber/python-docker-tutorial

-   image used:

    -   https://hub.docker.com/r/jupyter/pyspark-notebook
    -   https://github.com/jupyter/docker-stacks/blob/main/images/pyspark-notebook/Dockerfile
    -   https://jupyter-docker-stacks.readthedocs.io/en/latest/using/specifics.html

_local_

-   pyspark doesn't work with the latest 3.12 python and java 21 versions (as of may 2024) because of an incompatibility issue with `pyarrow`.

    see: https://stackoverflow.com/a/77318636/13045051

-   the jupyter team is using: python@3.11 and java@17.

    use a language runtime version manager like `asdf`.

    make sure that the workers use the same version of python as the driver. you can check this by running `pyspark` and reading the logs.

-   then install the following packages:

    ```bash
    brew install apache-spark

    python3 -m pip install pyspark 'pyspark[sql]' 'pyspark[ml]' 'pyspark[streaming]'
    ```
