FROM benchflow/base-images:envconsul-java8_dev

MAINTAINER Vincenzo FERME <info@vincenzoferme.it>

ENV SPARK_HOME /usr/spark
ENV SPARK_VERSION 1.5.1
ENV HADOOP_VERSION 2.6
ENV SPARK_TASKS_SENDER_VERSION v-dev
ENV DATA_TRANSFORMERS_VERSION v-dev
ENV ANALYSERS_VERSION v-dev
ENV PLUGINS_VERSION v-dev
ENV CONFIGURATION_FILTER 'data-transformers-config.json'

RUN apk --update add curl tar && \
	# Get spark-tasks-sender
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
    # Get data-transformers
    mkdir -p /app/data-transformers && \
    wget -q -O - https://github.com/benchflow/data-transformers/archive/$DATA_TRANSFORMERS_VERSION.tar.gz \
    | tar xz --strip-components=2 -C /app/data-transformers data-transformers-$DATA_TRANSFORMERS_VERSION/data-transformers && \
    # Get analysers
    mkdir -p /app/analysers && \
    wget -q -O - https://github.com/benchflow/analysers/archive/$ANALYSERS_VERSION.tar.gz \
    | tar xz --strip-components=2 -C /app/analysers analysers-$ANALYSERS_VERSION/analysers && \
    # Get plugins (configuration files)
    mkdir -p /app/data-transformers/conf && \
    wget -q -O - https://github.com/benchflow/sut-plugins/archive/$PLUGINS_VERSION.tar.gz \
    | tar xz --strip-components=1 -C ./app/data-transformers/conf --wildcards --no-anchored $CONFIGURATION_FILTER && \
    # Clean up
    apk del --purge curl tar && \
    rm -rf /var/cache/apk/*

COPY ./config /app/config
	
COPY ./services/300-spark-tasks-sender.conf /apps/chaperone.d/300-spark-tasks-sender.conf
 
EXPOSE 8080