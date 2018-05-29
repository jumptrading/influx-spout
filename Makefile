PROJECT ?= "influx-spout"
BUILT_ON ?= $(shell date --iso-8601)
VERSION ?= $(shell git describe --tags --always --dirty --match=v*)
DOCKER_NAME ?= "$(PROJECT)"
DOCKER_TAG ?= "0.1.$(VERSION)"
DOCKER_IMAGE ?= "$(DOCKER_NAME):$(DOCKER_TAG)"

ifeq ($(GOPATH),)
abort:
	$(error Ensure Go 1.9+ is available and the GOPATH env variable is set)
endif

all: deps check-git influx-spout influx-spout-tap


clean:
	go clean
	rm -f influx-spout influx-spout-tap reference.bench current.bench

check-git:
	@# See these files with:
	@#     git diff
	$(info Checking for a clean git working tree)
	@if [ "$(shell git diff-files --quiet --ignore-submodules --; echo $$?)" -ne 0 ]; then \
		(echo ERROR: unstaged changes in the working tree >&2; exit 1); \
	fi
	@# See these files with:
	@#     git diff --cached
	@if [ "$(shell git diff-index --cached --quiet HEAD --ignore-submodules --; echo $$?)" -ne 0 ]; then \
		(echo ERROR: Uncommitted changes in the index >&2; exit 1); \
	fi


deps:
	$(info Checking $(PROJECT) dependencies)
	go get -u github.com/golang/dep/cmd/dep
	${GOPATH}/bin/dep ensure

	@# Check for any untracked files under vendor/* after 'dep ensure'
	@if [ "$(shell git status --porcelain 2>/dev/null | grep -q '^?? vendor/'; echo $$?)" -eq 0 ]; then \
		(echo ERROR: uncommitted files under vendor/ >&2; exit 1); \
	fi


influx-spout:
	$(info Building $(PROJECT) version=$(VERSION) builtOn=$(BUILT_ON))
	@export CGO_ENABLED=0

	# Build a static version of influx-spout
	go build -a -tags netgo -installsuffix netgo -v -x -ldflags '-X main.version=$(VERSION) -X main.builtOn=$(BUILT_ON) -w -extldflags "-static"' ./cmd/influx-spout
	@ls -l influx-spout

influx-spout-tap:
	go build ./cmd/influx-spout-tap

docker: influx-spout influx-spout-tap
	$(info Building the docker image $(DOCKER_IMAGE) with $(PROJECT) $(VERSION))
	docker build -t "$(DOCKER_IMAGE)" .


.PHONY: test
test:
	./runtests -r small medium large


.PHONY: coverage
coverage:
	./runtests -c -r small medium large


.PHONY: benchmark
benchmark:
	./runtests -b -r small medium large
