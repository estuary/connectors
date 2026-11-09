# The builder stage of the Go connectors' Dockerfiles: the golang image with
# this repo's modules downloaded and every package and test compiled, so a
# connector build only compiles what it changed. Its two targets differ in
# their build cache, since `-tags nozstd` changes every package's cache key:
# `default` is compiled without tags, `nozstd` with `-tags nozstd`.
#
# Built from the repository root:
#   docker buildx build --platform linux/amd64 -f base-image-go/Dockerfile \
#     --target default -t ghcr.io/estuary/base-image-go:dev .
ARG GOLANG_IMAGE=golang:1.27-trixie

FROM ${GOLANG_IMAGE} AS modules

# Go's build cache keys include the package directory, so this must be the
# connector builds' WORKDIR.
WORKDIR /builder

# Builds read the extracted module sources and verify them against the zips'
# hashes, so the zips themselves only add to every pull.
COPY go.* ./
RUN go mod download \
    && find /go/pkg/mod/cache/download -name '*.zip' -delete \
    && du -sh /go/pkg/mod

# A package that fails to compile here, like source-db2-batch's without IBM's
# headers, fails in its own connector's build instead. `-exec true` links
# the test binaries without running them.
FROM modules AS default
RUN --mount=type=bind,target=/builder,rw \
    go build ./... > /dev/null 2>&1; \
    go test -exec true -run '^$' ./... > /dev/null 2>&1; \
    du -sh /root/.cache/go-build

FROM modules AS nozstd
RUN --mount=type=bind,target=/builder,rw \
    go build -tags nozstd ./... > /dev/null 2>&1; \
    go test -tags nozstd -exec true -run '^$' ./... > /dev/null 2>&1; \
    du -sh /root/.cache/go-build
