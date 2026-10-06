# Operator-observable logs

By default, a connector's logs flow only to the user's `ops/<tenant>/logs`
collection. They are _not_ shipped into Estuary's own observability pipeline
(the data-plane reactor's log stream, which is forwarded to Loki/Grafana),
because the full volume of connector logs is far too high to send there.

A connector can opt a _specific_ log line into that pipeline by attaching the
structured field `observable` set to `true`. The runtime forwards such lines into
the reactor's log stream, tagged with the task name, so operators can alert and
dashboard on them in Grafana.

This is for **rare, high-signal lines an operator would want to monitor** — for
example, a connector detecting that it has been rate-limited or blocked by an
upstream API. It is not for bulk or routine connector output. Forwarding is gated
by the reactor's configured log level, so a marked line below that level is still
dropped.

The line is still published to the user's `ops/<tenant>/logs` collection as usual;
`observable` only adds the operator-facing copy.

## Emitting an observable line

The marker is just a field named `observable` in the log line's structured
`fields`, with the JSON value `true`.

### Python (estuary-cdk)

Pass `observable=True` via the logger's `extra` mapping, which becomes a
structured field:

```python
log.warning("upstream API rate-limited us", extra={"observable": True, "endpoint": "/v2/sync"})
```

### Go / raw JSON

Connectors that emit `LOG_FORMAT=json` log lines include `observable` in `fields`:

```json
{"level":"warn","message":"upstream API rate-limited us","fields":{"observable":true,"endpoint":"/v2/sync"}}
```
