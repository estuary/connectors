# This dockerfile is used by all the connectors. The connector being built is passed as the
# `connector` build arg.
FROM debian:buster-slim

RUN mkdir /connector
ENV PATH=/connector:$PATH

RUN apt-get update && apt-get install -y ca-certificates && apt-get clean


# The name of the connector executable to use. Ex: "source-s3".
# Note that this connector binary will need to be added to .dockerignore or else the build will fail
ARG connector
# Build args are not expanded as part of the entrypoint, so we need to set an env variable from the
# arg, so we can use the env variable in the entrypoint.
ENV CONNECTOR=$connector
RUN addgroup connector && \
    adduser connector --ingroup connector --gecos "" --no-create-home --disabled-passwor
USER connector

COPY --chown=connector:connector parser/target/x86_64-unknown-linux-musl/release/parser /connector/parser
RUN chmod +x /connector/parser

COPY --chown=connector:connector $connector/$connector /connector/connector
RUN chmod +x /connector/connector

ENTRYPOINT ["/connector/connector"]
