FROM --platform=linux/amd64 debian:stable-slim AS debian

RUN apt-get update && apt-get install -y openssh-client ca-certificates curl

# Create a non-privileged "nonroot" user.
RUN useradd nonroot --create-home --shell /usr/sbin/nologin
RUN chown nonroot /home/nonroot

USER nonroot:nonroot

RUN mkdir /home/nonroot/.ssh
RUN chmod 700 /home/nonroot/.ssh
RUN chmod 755 /home/nonroot

# Ask the Go runtime to keep heap size below 900MiB. Our real memory limit is
# 1GiB so this is 10% headroom as recommended in https://go.dev/doc/gc-guide
ENV GOMEMLIMIT=900MiB

USER root
