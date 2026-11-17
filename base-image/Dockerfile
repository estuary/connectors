FROM --platform=linux/amd64 debian:trixie-slim AS debian

# Upgrade the packages that ship in debian:trixie-slim as well, so security
# fixes published after that image was cut are picked up on every rebuild.
# `apt-get install` alone leaves already-installed packages at the image's
# version.
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
 && apt-get install -y openssh-client ca-certificates curl tzdata tzdata-legacy

# Create a non-privileged "nonroot" user.
RUN useradd nonroot --create-home --shell /usr/sbin/nologin
RUN chown nonroot /home/nonroot

USER nonroot:nonroot

RUN mkdir /home/nonroot/.ssh
RUN chmod 700 /home/nonroot/.ssh
RUN chmod 755 /home/nonroot

# Fallback Go GC memory limit, 90% of the default 1GiB container allocation,
# per the recommendations in https://go.dev/doc/gc-guide.
#
# Overridden dynamically via `common.ConfigureMemoryLimit()` which reads the
# actual cgroup `memory.max` and uses 90% of that instead.
ENV GOMEMLIMIT=900MiB

USER root
