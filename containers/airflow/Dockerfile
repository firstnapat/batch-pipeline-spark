FROM apache/airflow:3.1.8

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY quarto.sh /
RUN cd / && bash /quarto.sh

COPY setup_conn.py $AIRFLOW_HOME

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o /tmp/spark.tgz && \
    mkdir -p /opt/spark && \
    tar xzf /tmp/spark.tgz --directory /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz

ENV JAVA_HOME='/usr/lib/jvm/java-17-openjdk-amd64'
ENV PATH=$PATH:$JAVA_HOME/bin
ENV SPARK_HOME='/opt/spark'
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
