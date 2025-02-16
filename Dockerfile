FROM python:3.11-slim AS base

ARG SPARK_VERSION=3.5.4
ARG HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH \
    PYTHONHASHSEED=1 \
    SPARK_LOG_DIR="$SPARK_HOME/logs" \
    SPARK_MASTER_LOG="$SPARK_HOME/logs/spark-master.out" \
    SPARK_WORKER_LOG="$SPARK_HOME/logs/spark-worker.out"

# Installer Java pour Spark
RUN apt-get update && apt-get install -y wget curl \
    openjdk-17-jdk libz-dev libssl-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    mkdir -p $SPARK_HOME && \
    tar -xf apache-spark.tgz -C $SPARK_HOME --strip-components=1 && \
    rm apache-spark.tgz

## Installer les dépendances Python
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

WORKDIR $SPARK_HOME
RUN mkdir -p "$SPARK_HOME/logs"

#COPY config/*.conf conf/.
COPY config conf
COPY start-spark.sh /

RUN echo 'alias l="ls -lA --color --group-directories-first"' >> /root/.bashrc

EXPOSE 8080 7077 4040
CMD ["/bin/bash", "/start-spark.sh"]
