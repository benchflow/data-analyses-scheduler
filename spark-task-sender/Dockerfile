FROM golang
 
ADD src/sender /go/src/sender
ADD config /go

RUN go get github.com/Shopify/sarama
RUN go install sender

RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-1.5.1-bin-hadoop2.6.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-1.5.1-bin-hadoop2.6 spark

ENV SPARK_HOME /usr/local/spark

ENTRYPOINT ["/bin/bash"]