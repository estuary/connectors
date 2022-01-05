FROM rustembedded/cross:x86_64-unknown-linux-musl-0.2.1

RUN apt-get update && \
    apt-get install --assume-yes libssl-dev

RUN cp /usr/include/x86_64-linux-gnu/openssl/* /usr/include/openssl
RUN cp -r /usr/include/x86_64-linux-gnu /usr/include/x86_64-linux-musl
