FROM benchflow/base-images:dns-envconsul-java8_dev

MAINTAINER Vincenzo FERME <info@vincenzoferme.it>

ENV SPARK_HOME /usr/spark
ENV SPARK_VERSION 1.5.1
ENV PYSPARK_PYTHON python2.7
ENV HADOOP_VERSION 2.6
ENV SPARK_TASKS_SENDER_VERSION v-dev
ENV DATA_TRANSFORMERS_VERSION v-dev
ENV ANALYSERS_VERSION v-dev
ENV PLUGINS_VERSION v-dev
#TODO: use this, currently we use a local configuration for testing purposes
# ENV CONFIGURATION_FILTER 'config.json'

# TODO: remove python, when Spark will be used outside of the container
RUN apk --update add curl tar python && \
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
    #TODO: use this, currently we use a local configuration for testing purposes
    # mkdir -p /app/data-transformers/conf && \
    # wget -q -O - https://github.com/benchflow/sut-plugins/archive/$PLUGINS_VERSION.tar.gz \
    # | tar xz --strip-components=1 -C ./app/data-transformers/conf --wildcards --no-anchored $CONFIGURATION_FILTER && \
    # Clean up
    apk del --purge curl tar && \
    rm -rf /var/cache/apk/*

COPY ./configuration /app/configuration
# TODO: Remove this
COPY ./conf /app/data-transformers/conf

COPY ./services/envcp/config.tpl /app/config.tpl
	
COPY ./services/300-spark-tasks-sender.conf /apps/chaperone.d/300-spark-tasks-sender.conf

#TODO: remove, when Spark will be used as a service outside of this container
COPY ./services/400-clean-tmp-folder.conf /apps/chaperone.d/400-clean-tmp-folder.conf

#TODO: remove, when Spark will be used as a service outside of this container
# disables the Spark UI when launching scripts (http://stackoverflow.com/questions/33774350/how-to-disable-sparkui-programmatically/33784803#33784803)
RUN cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf \
    && sed -i -e '$aspark.ui.enabled false' $SPARK_HOME/conf/spark-defaults.conf

# adds Alpine's testing repository and install scripts dependencies (py-numpy, py-scipy)
RUN sed -i -e '$a@testing http://dl-4.alpinelinux.org/alpine/edge/testing' /etc/apk/repositories \
    && apk --update add py-numpy@testing py-scipy@testing
 
EXPOSE 8080