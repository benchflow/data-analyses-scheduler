REPONAME = data-analyses-scheduler
DOCKERIMAGENAME = benchflow/$(REPONAME)
VERSION = dev
GOPATH_SAVE_RESTORE:=$(shell pwd):${GOPATH}

.PHONY: all build_release install_release

all: build_release install_release

save_dependencies:
	# TODO: figure out how to get the following work, currently we add the dependencies to the Godeps.json manually
	# - refer to the following issue: https://github.com/benchflow/benchflow/issues/33
	# cd src/cloud/benchflow/$(REPONAME)/ && \
	# GOPATH=$(GOPATH_SAVE_RESTORE) godep save ./... && \
	# rm -rf ../../../../Godeps/*.* && \
	# rm -rf ../../../../Godeps && \
	# mv Godeps/ ../../../.. && \
	# cd ../../../..

restore_dependencies:
	# TODO: make it working with all the folders
	rm -rf Godeps/_workspace/src/golang.org/*
	rm -rf Godeps/_workspace/src/github.com/*
	rm -rf Godeps/_workspace/src/gopkg.in/*
	GOPATH=$(GOPATH_SAVE_RESTORE) godep restore ./...
	# TODO: make it working with all but cloud folder
	# TODO. NOTE: github.com/wvanbergen/kafka and its dependecies not automatically downloaded with Godeps because of the github.com/wvanbergen/kafka repository structure
	# For the same reason, the error is ignored (- at the beginning of the command)
	-GOPATH=$(GOPATH_SAVE_RESTORE) go get github.com/wvanbergen/kafka
	mv src/golang.org/* Godeps/_workspace/src/golang.org
	mv src/github.com/* Godeps/_workspace/src/github.com
	mv src/gopkg.in/* Godeps/_workspace/src/gopkg.in
	rm -rf src/golang.org
	rm -rf src/github.com
	rm -rf src/gopkg.in

# TODO: figure out how to get vendor dependencies committed with the standard git flow
# - refer to the following issue: https://github.com/benchflow/benchflow/issues/33
prepare_to_commit:
	cd Godeps && \
	find . | grep .git/ | xargs rm -rf && \
	git add --all -f .

clean:
	go clean -i ./...
	rm -rf Godeps/_workspace/pkg

build:
	GOPATH=$(GOPATH_SAVE_RESTORE) godep go build -v ./...

build_release:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 godep go build -ldflags '-s' -v ./...

install:
	godep go install -v ./...
	
install_release:
	godep go install -v ./...

test:
	godep go test ./...

build_container:
	docker build -t $(DOCKERIMAGENAME):$(VERSION) -f Dockerfile .

build_container_local:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 godep go build -ldflags '-s' -o bin/$(REPONAME)_linux -v ./...
	docker build -t $(DOCKERIMAGENAME):$(VERSION) -f Dockerfile.test .
	rm bin/$(REPONAME)_linux

test_container_local:
	docker run -d -p 8080:8080 \
	-e "ENVCONSUL_CONSUL=$(ENVCONSUL_CONSUL_IP):$(ENVCONSUL_CONSUL_PORT)" \
	-e "KAFKA_HOST=zookeeper" \
	-e "KAFKA_PORT=2181" \
	-e "SPARK_MASTER=local[*]" \
	-e "CASSANDRA_IP=$(CASSANDRA_IP)" \
	-e "MINIO_IP=$(MINIO_IP)" \
	--link kafkadocker_zookeeper_1:zookeeper --link kafkadocker_kafka_1:kafka \
	--name $(REPONAME) $(DOCKERIMAGENAME):$(VERSION)

rm_container_local:
	docker rm -f -v $(REPONAME)