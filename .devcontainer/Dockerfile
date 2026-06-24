FROM apache/spark:3.5.1

USER root

ENV DEBIAN_FRONTEND=noninteractive

# Install Python packages
RUN pip3 install requests PyYAML python-dotenv trino sqlalchemy-trino

# Download Iceberg JAR
RUN curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar

# Create Warehouse directory with full permissions
RUN mkdir -p /project/Warehouse && chmod -R 777 /project/Warehouse

ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:/opt/spark/sbin:$PATH"
ENV PYTHONPATH="/project:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /project