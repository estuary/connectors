.DEFAULT_GOAL := test-all

build_dir = .build
version = $(shell git describe --always --dirty --tags)

# Enumeration of all the connectors. These names are expected to match the name of the directories.
connectors = source-s3 source-kinesis

parser_target_dir = parser/target/x86_64-unknown-linux-musl/release
parser = $(parser_target_dir)/parser
# Add the parser target dir to the path so that it's available during tests
export PATH:=${CURDIR}/$(parser_target_dir):$(PATH)
parser_cargo_args = --release --manifest-path parser/Cargo.toml --target x86_64-unknown-linux-musl

$(parser): parser
	cargo build $(parser_cargo_args)

.PHONY: test-parser
test-parser:
	cargo test $(parser_cargo_args)

.PHONY: test-source-s3
test-source-s3:
	cd source-s3 && go test -v ./...

.PHONY: test-source-kinesis
test-source-kinesis:
	cd source-kinesis && go test -tags kinesistest -v ./...

.PHONY: test-go-types
test-go-types:
	cd go-types && go test -v ./...

.PHONY: test-all
test-all: test-parser test-go-types test-source-kinesis test-source-s3

# build_dir should be used as an order-only pre-requisite, because otherwise it would always cause
# targets to rebuild if any file in the build directory was newer than the current target.
$(build_dir):
	mkdir -p $(build_dir)

build_connectors = $(addprefix $(build_dir)/build-,$(connectors))

$(build_connectors): $(build_dir)/build-%: $(parser) % go-types | $(build_dir)
	cd $* && go build
	docker build -t ghcr.io/estuary/$*:$(version) --build-arg connector=$* .
	@# This file is only used so that make can correctly determine if targets need rebuilt
	echo $(version) > $@

push_connectors = $(addprefix $(build_dir)/push-,$(connectors))

$(push_connectors): $(build_dir)/push-%: $(build_dir)/build-%
	docker push ghcr.io/estuary/$*:$(version)
	@# This file is only used so that make can correctly determine if targets need rebuilt
	echo $(version) > $@

.PHONY: build-all
build-all: $(build_connectors)

.PHONY: push-all
push-all: $(push_connectors)

