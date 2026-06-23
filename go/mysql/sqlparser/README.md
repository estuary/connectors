# sqlparser

A MySQL-dialect SQL parser, extracted from the [Vitess](https://github.com/vitessio/vitess)
`go/vt/sqlparser` package and modified by Estuary Technologies, Inc.

## Why

`source-mysql` parses replication-stream DDL to keep its table metadata in sync
with the source database. Upstream Vitess does not support all DDL queries which
might possibly be observed in real MySQL/MariaDB instances. By extracting the
core parser we can more easily extend the grammar as needed to support the DDL
we see in production.

## Modifications

We kept the core parse logic and stripped out everything that isn't needed for
pure string-to-AST parsing, including most AST visitors and normalization and
rewriting. Also switched from Vitess `goyacc` to `golang.org/x/tools/cmd/goyacc`.

## License

Derived from Apache-2.0 Vitess source and remains under Apache 2.0 (see `LICENSE`)
Copied files keep their upstream headers; modified files carry a notice as required
by the license.
