.DEFAULT_GOAL := test-all

build_dir = .build
version = $(shell git describe --always --dirty --tags)

.PHONY: print-version
print-version:
	@echo $(version)

# Enumeration of all the connectors. These names are expected to match the name of the directories.
connectors = \
	source-gcs \
	source-hello-world \
	source-kinesis \
	source-s3

parser_target_dir = parser/target/x86_64-unknown-linux-musl/release
parser = $(parser_target_dir)/parser
# Add the parser target dir to the path so that it's available during tests
export PATH:=${CURDIR}/$(parser_target_dir):$(PATH)
parser_cargo_args = --release --manifest-path parser/Cargo.toml --target x86_64-unknown-linux-musl

$(parser): parser
	cargo build $(parser_cargo_args)

# TODO: talk to Phil about make again
parser_cli: $(parser)

.PHONY: test-filesource
test-filesource:
	go test -v ./filesource/...

.PHONY: test-parser
test-parser:
	cargo test $(parser_cargo_args)
	go test -v ./parser/...

.PHONY: test-source-kinesis
test-source-kinesis:
	go test -tags kinesistest -v ./source-kinesis/...

.PHONY: test-all
test-all: test-filesource test-parser test-source-kinesis

# build_dir should be used as an order-only pre-requisite, because otherwise it would always cause
# targets to rebuild if any file in the build directory was newer than the current target.
$(build_dir):
	mkdir -p $(build_dir)

$(build_dir)/version: | $(build_dir)
	@if [ -f $(build_dir)/version ] && [ "$$(cat $(build_dir)/version)" == "$(version)" ]; then \
		echo "version up to date"; \
	else \
		echo "$(version)" > $(build_dir)/version; \
	fi


build_connectors = $(addprefix $(build_dir)/build-,$(connectors))

# Second expansion is needed to defer the expansion of $(shell find % -type f) until after the rule
# is matched. This ensures that the connector will be rebuilt if any file within its source
# directory is modified.
# TODO(johnny): How should this change for the protocols refactor ?
.SECONDEXPANSION:
$(build_connectors): $(build_dir)/build-%: $(parser) % filesource $(shell find filesource -type f) $$(shell find % -type f) $(build_dir)/version | $(build_dir)
	docker build --file $* -t ghcr.io/estuary/$*:$(version)
	@# This file is only used so that make can correctly determine if targets need rebuilt
	echo $(version) > $@

.PHONY: build-all
build-all: $(build_connectors)

integration_test_connectors = $(addprefix int-test-,$(connectors))
.PHONY: $(integration_test_connectors)
# The 'int-test-%' here is extracting the connector name from the full target name.
# The actual pre-requisites come after the second ':'.
# The | separates regular pre-requisites from "order-only" pre-requisites. In this case, it prevents
# the integration tests from being re-run due to the timestamp on our build directory being updated.
$(integration_test_connectors): int-test-%: $(build_dir)/build-% | $(build_dir)
	if [ -d tests/$* ]; then \
		CONNECTOR=$* VERSION=$(version) ./tests/run.sh; \
	fi

.PHONY: integration-tests
integration-tests: $(integration_test_connectors)
