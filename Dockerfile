FROM astrocrpublic.azurecr.io/runtime:3.0-8

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless wget curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

RUN mkdir -p /tmp/spark && chmod -R 777 /tmp/spark
ENV TMPDIR=/tmp/spark
ENV SPARK_LOCAL_DIRS=/tmp/spark

# Hadoop + AWS SDK compatível
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER astro
