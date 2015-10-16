FROM golang
 
ADD src/sender /go/src/sender
ADD config.json /go

RUN go get github.com/Shopify/sarama
RUN go install sender

ENTRYPOINT /go/bin/sender

