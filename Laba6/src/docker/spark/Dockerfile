FROM spark:4.1.1-python3

# Spark MLlib (pyspark.ml) requires numpy, which is not present in the base image.
USER root

RUN pip install --no-cache-dir numpy

COPY postgresql-42.7.4.jar /opt/spark/jars/postgresql-42.7.4.jar
RUN chmod 644 /opt/spark/jars/postgresql-42.7.4.jar

USER spark
