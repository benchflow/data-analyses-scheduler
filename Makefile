REPONAME = spark-tasks-sender
DOCKERIMAGENAME = benchflow/$(REPONAME)
VERSION = dev
GOPATH_SAVE_RESTORE:=`pwd`"/Godeps/_workspace"

ENVCONSUL_CONSUL_IP = http://195.176.181.55
ENVCONSUL_CONSUL_PORT = 8500
KAFKA_IP = 195.176.181.55
KAFKA_PORT = 9092
CASSANDRA_IP = 195.176.181.55
MINIO_IP = 195.176.181.55

.PHONY: all build_release 

all: build_release

save_dependencies:
	cd src/cloud/benchflow/$(REPONAME)/ && \
	GOPATH=$(GOPATH_SAVE_RESTORE) godep save ./... && \
	rm -rf ../../../../Godeps/*.* && \
	rm -rf ../../../../Godeps && \
	mv Godeps/ ../../../.. && \
	cd ../../../..

restore_dependencies: 
	GOPATH=$(GOPATH_SAVE_RESTORE) godep restore ./...

clean:
	go clean -i ./...
	rm -rf Godeps/_workspace/pkg

build:
	godep go build -o bin/$(REPONAME) -v ./...

build_release:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 godep go build -ldflags '-s' -o bin/$(REPONAME) -v ./...

install:
	godep go install -v ./...
	mv bin/$(REPONAME) bin/$(REPONAME)

test:
	godep go test ./...

build_container_local:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 godep go build -ldflags '-s' -o bin/$(REPONAME)_linux -v ./...
	docker build --no-cache -t $(DOCKERIMAGENAME):$(VERSION) -f Dockerfile.test .
	rm bin/$(REPONAME)_linux

test_container_local:
	docker run -d -p 8080:8080 \
	-e "ENVCONSUL_CONSUL=$(ENVCONSUL_CONSUL_IP):$(ENVCONSUL_CONSUL_PORT)" \
	-e "KAFKA_IP=$(KAFKA_IP)" \
	-e "KAFKA_PORT=$(KAFKA_PORT)" \
	-e "SPARK_MASTER=local[*]" \
	-e "CASSANDRA_IP=$(CASSANDRA_IP)" \
	-e "MINIO_IP=$(MINIO_IP)" \
	--name $(REPONAME) $(DOCKERIMAGENAME):$(VERSION)

rm_container_local:
	docker rm -f -v $(REPONAME)