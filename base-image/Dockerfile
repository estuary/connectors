FROM --platform=linux/amd64 debian:trixie-slim AS debian

RUN apt-get update && apt-get install -y --no-install-recommends openssh-client ca-certificates curl tzdata tzdata-legacy

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

# End as root so connector images built FROM this one can still install
# packages. Each of those must switch to `USER nonroot:nonroot` as its last
# step, which Trivy checks per connector Dockerfile.
# trivy:ignore:DS-0002
USER root
