# https://github.com/docker-library/python/blob/master/3.11/bookworm/Dockerfile#L9-L126
ARG BASE_IMAGE=python:3.11-bookworm

# NOTE NetSuite ODBC is only compatible with linux/x86_64
FROM --platform=linux/x86_64 ${BASE_IMAGE}

# Flowctl will crash with confusing error messages without these
LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL FLOW_RUNTIME_CODEC=json

ENV \
  # python:
  PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PYTHONDONTWRITEBYTECODE=1 \
  # pip:
  PIP_NO_CACHE_DIR=1 \
  PIP_DISABLE_PIP_VERSION_CHECK=1 \
  PIP_DEFAULT_TIMEOUT=100 \
  PIP_ROOT_USER_ACTION=ignore

# add system libraries
ADD docker/install.sh ./
RUN ./install.sh

# Create a non-privileged "nonroot" user.
# Pulled from estuary:base-image
RUN useradd nonroot --create-home --shell /usr/sbin/nologin
RUN chown nonroot /home/nonroot

ENV PATH="/home/nonroot/.local/bin/:${PATH}"
ENV APP_DIR=/home/nonroot/app
ENV PYTHON_ENV=production

# NOTE this is only needed while we are running a custom build of pyodbc
RUN apt-get update && apt-get install unixodbc-dev --yes && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# run the rest of the docker container in a nonroot user
USER nonroot:nonroot

RUN mkdir ${APP_DIR}
RUN mkdir /home/nonroot/.ssh
RUN chmod 700 /home/nonroot/.ssh
RUN chmod 755 /home/nonroot

WORKDIR $APP_DIR

# this path is recommended by the NetSuite documentation and is assumed in the shell scripts
# run out of this directory by the entrypoint script
COPY ./netsuite_odbc_driver /opt/netsuite/odbcclient

# install python packages
ADD poetry.lock pyproject.toml .tool-versions docker/build.sh ./
# TODO temporarily copy over the netsuite py lib until the finalized library is published
# ADD --chown=nonroot:nonroot ./netsuite-python ./netsuite-python
RUN ./build.sh

# copy the rest of the application
COPY ./ ./

# although the user is nonroot, files are copied as root, so we have to force ownership
USER root
RUN mkdir -p $APP_DIR && chown -R nonroot:nonroot $APP_DIR
USER nonroot:nonroot

# netsuite driver requires certain ODBC related environment variables to be set
# this ensures they are always properly configured before any command in run
ADD docker/entrypoint.sh ./

ENV CONNECTOR_NAME=source-netsuite

# NOTE flow overrides the `entrypoint` in a Dockerfile, so we cannot use that for `entrypoint.sh` here
#      https://github.com/estuary/flow/blob/119039cfef3d866119e30c5fa3c4f08de223842b/crates/connector-init/src/inspect.rs#L28-L36
CMD [ "./entrypoint.sh", "python", "-m", "source_netsuite" ]
