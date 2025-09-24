FROM astrocrpublic.azurecr.io/runtime:3.0-8

USER root

# Java e utilitários
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless wget curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Spark temporário
RUN mkdir -p /tmp/spark && chmod -R 777 /tmp/spark
ENV TMPDIR=/tmp/spark
ENV SPARK_LOCAL_DIRS=/tmp/spark

# Diretório para JARs Hadoop + AWS
RUN mkdir -p /opt/spark/jars

# Baixar JARs Hadoop + AWS
RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar

# Garantir que Spark use os JARs em qualquer contexto
ENV SPARK_CLASSPATH=/opt/spark/jars/*
ENV SPARK_JARS=/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/wildfly-openssl-1.0.7.Final.jar
ENV PYSPARK_SUBMIT_ARGS="--jars $SPARK_JARS --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem pyspark-shell"

# Instalar requirements Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER astro
