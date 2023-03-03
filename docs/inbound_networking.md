# Inbound networking for connectors

All connectors are able to make ougoing connections to the public internet, which they do to connect to databases, APIs, etc. Additionally connectors may now also accept _incoming_ connections on an opt-in basis. This can be used for a variety of purposes, including but not limited to allowing external clients to "push" data into Flow, or for exposing debug endpoints over HTTP.

## How to expose ports

The simplest way to enable incoming connections is to simply add an `EXPOSE` directive to the connector's `Dockerfile`. For example, `EXPOSE 8080`. Doing only this, the connector will be able to receive authorized HTTP requests. These ports are considered "private", because they can only receive requests from users who are authorized to access the specific task that the connector is serving.

An authorized request here means that the request has a valid data-plane JWT in an `Authorization: Bearer ` header. Note that you can get such a token using `flowctl auth data-plane-access-token --prefix myPrefix/`.

Private ports currently only support HTTP/1.1 and HTTP2 protocols. The default assumes that the connector will use only HTTP/1.1, though the proxy server will allow clients to speak either protocol.

## Public ports

To make a port public, you must add a [`LABEL`](https://docs.docker.com/engine/reference/builder/#label) to the Dockerfile. The label takes for form of `dev.estuary.port-public.<portNum>=true` where `<portNum>` is the port number that has been provided to the `EXPOSE` directive. For example, to expose port 8080 publicly, you'd have the following directives to your Dockerfile:

```
EXPOSE 8080
LABEL dev.estuary.port-public.8080=true
```

**Important bits about public ports:**

- Public ports allow connections from any client, without any form of authentication.
- The expectation is that the connector itself should perform any necessary authentication and authorization.
- Any TCP-based protocol may be used with a public port, not just HTTP.

## Protocols

You can configure the protocol that's associated with the port using a similar image label. This label takes the form of `dev.estuary.port-proto.<portNum>=<proto>`. For example, to configure port 567 to use the `ftp` protocol, you could add the following to your Dockerfile:

```
EXPOSE 567

LABEL dev.estuary.port-proto.567=ftp \
	  dev.estuary.port-public.567=true
```

**Important bits about protocols:**

- If the protocol is set to anything other than `http/1.1` or `h2`, then the port must be public. This is because our data-plane-gateway has no way to authenticate non-http requests.
- The exact protocol value you supply will be used during the TLS ALPN negotiation with the client.
- Connectors _should_ use [official ALPN protocol designations](https://www.iana.org/assignments/tls-extensiontype-values/tls-extensiontype-values.xhtml#alpn-protocol-ids) whenever possible.
- As a special case, if the connector doesn't specify a protocol, the default of `[h2, http/1.1]` will be used with ALPN.

## Shard labels

The set of exposed ports and related labels from the docker image will be translated into Shard labels by the build process. The shard labels are used exclusively to determine the runtime configuration.

## Limitations and caveats

- Only TCP is supported for incoming network traffic. Connectors cannot receive incoming UDP packets.
- Clients must always use TLS when communicating with the gateway. Generally, connectors themselves should not use TLS, as that will be offloaded to the gateway.
- It's unclear how exactly we should approach exposing protocols like Postgres that use STARTLS.
- Labels for ports that are not exposed will be ignored

