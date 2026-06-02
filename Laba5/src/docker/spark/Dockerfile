FROM spark:4.1.1-python3

# Spark MLlib (pyspark.ml) requires numpy, which is not present in the base image.
USER root

RUN pip install --no-cache-dir numpy

USER spark
