FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk curl procps

RUN curl -L https://downloads.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz | tar zx -C /opt/
ENV SPARK_HOME=/opt/spark-3.4.0-bin-hadoop3
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow
RUN pip install pyspark