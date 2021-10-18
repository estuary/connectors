VERSION = $(shell git describe --dirty --always --tags --long)
DATE    = $(shell date +%F-%T-%Z)
PACKAGE = $(shell go list -m -f '{{.Path}}')

.PHONY: default
default: flowsim

.PHONY: flowsim
flowsim:
	# Compiling...
	go build -ldflags "-X main.Version=${VERSION} -X main.BuildDate=${DATE}" -o flowsim flowsim.go

.PHONY: test
test: tools mocks
	go test -cover ./...
