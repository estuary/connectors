EXECUTABLE := mt
VERSION = $(shell git describe --dirty --always --tags --long)
DATE    = $(shell date +%F-%T-%Z)

.PHONY: default
default: ${EXECUTABLE}

.PHONY: ${EXECUTABLE}
${EXECUTABLE}:
	# Compiling...
	MBP=go.gazette.dev/core/mainboilerplate
	go build -ldflags "-X $${MBP}.Version=${VERSION} -X $${MBP}.BuildDate=${DATE}" -o ${EXECUTABLE}

.PHONY: test
test: tools mocks
	go test -cover ./...
