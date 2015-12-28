FROM benchflow/base-images:envconsul-java8_dev

MAINTAINER Vincenzo FERME <info@vincenzoferme.it>

ENV SPARK_HOME /usr/spark
ENV SPARK_VERSION 1.5.1
ENV HADOOP_VERSION 2.6
ENV SPARK_TASKS_SENDER_VERSION v-dev

RUN apk --update add wget gzip curl && \
    wget -q --no-check-certificate -O /app/spark-tasks-sender https://github.com/benchflow/spark-tasks-sender/releases/download/$SPARK_TASKS_SENDER_VERSION/spark-tasks-sender && \
    chmod +x /app/spark-tasks-sender && \
    # Install Spark
    curl \
	--location \
	--retry 3 \
	http://d3kbcqa49mib13.cloudfront.net/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
	| gunzip \
	| tar x -C /usr/ && \
    ln -s /usr/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /usr/spark && \
    apk del --purge wget curl && \
    rm -rf /var/cache/apk/*

COPY ./config /app/config
	
COPY ./services/300-spark-tasks-sender.conf /apps/chaperone.d/300-spark-tasks-sender.conf
 
EXPOSE 8080