.DEFAULT_GOAL := testAll


parser = parser/target/release/parser
export PATH:=${CURDIR}/parser/target/release:$(PATH)
PARSER_CARGO_ARGS = --release --manifest-path parser/Cargo.toml --target x86_64-unknown-linux-musl

$(parser):
	cargo build $(PARSER_CARGO_ARGS)

.PHONY: testParser
testParser:
	cargo test $(PARSER_CARGO_ARGS)

.PHONY: testSourceKinesis
testSourceKinesis:
	cd source-kinesis && go test -tags kinesistest -v ./...

.PHONY: testAll
testAll: testParser testSourceKinesis

