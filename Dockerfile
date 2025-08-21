FROM astrocrpublic.azurecr.io/runtime:3.0-8

# Switch to root to install utilities
USER root

# Instalar Java 17, wget e curl
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless wget curl && \
    rm -rf /var/lib/apt/lists/*

# Configurar JAVA
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Garantir que o Spark carregue o conector Hadoop AWS compatível
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"

# Copiar e instalar pacotes Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Voltar ao usuário default do Airflow
USER astro
