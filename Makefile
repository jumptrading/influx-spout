BUILT_ON ?= $(shell date --iso-8601)
VERSION ?= $(shell git rev-parse HEAD)

ifeq ($(GOROOT),)
abort:
	@echo Ensure golang 1.9+ is available and the GOROOT env variable is set
endif
		
all: influx-spout

clean:
	go clean

influx-spout: deps
	@export GOPATH=`pwd`
	@echo "Building influx-spout version=$(VERSION) builtOn=$(BUILT_ON)"
	go build -v -x -ldflags "-X main.version=$(VERSION) -X main.builtOn=$(BUILT_ON)"
	@ls -l influx-spout

.PHONY: test
test:
	./runtests -r small medium large

.PHONY: coverage
coverage:
	./runtests -c -r small medium large

.PHONY: benchmark
benchmark:
	./runtests -b -r small medium large

deps:
	@echo "Getting influx-spout dependencies"
	go get -u github.com/golang/dep/cmd/dep
	${GOPATH}/bin/dep ensure
