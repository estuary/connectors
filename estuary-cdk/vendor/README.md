# Vendored Airbyte CDK

A pruned, in-tree fork of the Airbyte Python CDK, carried so that `estuary_cdk.shim_airbyte_cdk`
and the connectors built on it are not bound to the ancient dependency pins that
`airbyte-cdk` 0.52.x imposes.

## Provenance

| Package | Upstream version | License |
| --- | --- | --- |
| `airbyte_cdk` | [`airbyte-cdk` 0.52.10](https://pypi.org/project/airbyte-cdk/0.52.10/) | MIT |
| `airbyte_protocol` | [`airbyte-protocol-models` 0.4.2](https://pypi.org/project/airbyte-protocol-models/0.4.2/) | MIT |

Both are MIT licensed, Copyright (c) 2020 Airbyte. The license text is in `LICENSE.txt`.

`airbyte-protocol-models` was previously a transitive dependency of `airbyte-cdk`; it is folded
in here because it was the source of the `pydantic<2` constraint, and because `airbyte_cdk.models`
is a thin re-export of it.

## Why vendored

`airbyte-cdk` 0.52.10 pins `pydantic>=1.10.8,<2.0.0`, `jsonschema>=3.2.0,<3.3.0`,
`Jinja2>=3.1.2,<3.2.0`, `dpath>=2.0.1,<2.1.0`, `airbyte-protocol-models==0.4.2` and several
exact versions besides. Those ceilings collide with the security floors that Dependabot raises
on the connectors, and upgrading to a current `airbyte-cdk` is not an option: the connectors
depend on the pre-6.x `HttpStream` API, which no longer exists upstream.

## What was kept

Only what the connectors in this repository actually import, computed as the transitive closure
of their `airbyte_cdk` / `airbyte_protocol` imports and then pruned to a fixpoint. The result is
roughly a fifth of the distributed package:

| Removed | Reason |
| --- | --- |
| `sources/declarative` | The low-code YAML framework. No connector here uses it. Sole user of Jinja2. |
| `sources/file_based` | File-based sources. Unused. |
| `destinations` | Destination support, including the langchain/openai vector-db extras. Unused. |
| `connector_builder` | Connector Builder server. Unused. |
| `sources/singer`, `sources/deprecated`, `sources/embedded`, `sources/streams/concurrent` | Unused. |
| `utils/schema_inferrer.py` | Unused. Sole user of `genson`. |
| `sources/utils/schema_models.py` | Unused, and the gnarliest pydantic-v1 code in the package. |
| `sources/config.py` (`BaseConfig`) | Unused. |
| `models/well_known_types.py` | Unused protocol primitives. |
| `Connector`, `AirbyteSpec`, `MultipleTokenAuthenticator`, `AlwaysLogSliceLogger`, `LogAppenderMessageRepositoryDecorator`, `ObservedDict`/`ConfigObserver`, `expand_refs`/`rename_key`, `log_by_prefix` | Individually unreferenced. |
| `@deprecated` decorators | Warning-only; dropping them removes the `Deprecated` dependency. |

`airbyte_protocol/models/airbyte_protocol.py` was kept byte-identical to upstream through the
vendoring, so that the fork could be shown equivalent before anything moved. The pydantic v2
migration edited it, and it no longer matches upstream. It needed no regenerating: its v1
surface was `class Config: extra = Extra.allow`, repeated on twenty-two models, one
`min_items` (now `min_length`), and two `AnyUrl` fields (see below).

## Dependencies

The subset needs `backoff`, `dpath`, `jsonref`, `jsonschema`, `pendulum`, `pydantic`, `PyYAML`,
`requests` and `requests-cache`.

`airbyte-cdk` 0.52.10 declares seventeen runtime requirements; the subset needs nine of them.
The eight it drops are `airbyte-protocol-models` (vendored here), `cachetools`, `Deprecated`,
`genson`, `isodate`, `Jinja2`, `python-dateutil` and `wcmatch` — and with `wcmatch` goes its
own dependency `bracex`.

Six of those leave every connector lock file outright. `cachetools` and `python-dateutil`
remain wherever `google-auth` or `pendulum` still require them. `Jinja2` is gone from all
fourteen: the three that declared it directly as a CVE floor (`source-jira-legacy`,
`source-linkedin-ads-v2`, `source-zendesk-support`) had nothing left in their graphs that
needed it, so those pins went with it.

### Constraints

The versions in `estuary-cdk/pyproject.toml` are the fork's own. Vendoring landed with
upstream's pins carried over verbatim; relaxing them was the point of the exercise:

| | `airbyte-cdk` 0.52.10 | now | what had to change |
| --- | --- | --- | --- |
| `dpath` | `>=2.0.1,<2.1.0` | `^2.1` | 2.1 moved `dpath.util.*` to the package top level. The two call sites now use `dpath.get`/`dpath.new`; `dpath.options` is untouched. |
| `jsonref` | `>=0.2,<1.0` | `^1.1` | Nothing. `JsonRef.replace_refs` keeps its signature in 1.x, and still returns the proxy type `resolve_ref_links` tests for. |
| `jsonschema` | `>=3.2.0,<3.3.0` | `^4` | Nothing. `validate`, `validators.create` and `Draft7Validator.VALIDATORS`/`META_SCHEMA` are unchanged. |
| `pydantic` | `>=1.10.8,<2.0.0` | `^2` | Twenty-two `class Config` blocks in `airbyte_protocol.py`, and fifteen call sites in `airbyte_cdk` (`parse_obj`, `.dict()`, `.json()`). See below. |

The major bounds are deliberate: the fork is never re-synced upstream, so a breaking major
has to be absorbed by editing `vendor/` by hand.

The `jsonschema` bump also drops `setuptools` (see below) and `pyrsistent` from every lock,
in exchange for `referencing` and `rpds-py`.

One deprecated API remains: `sources/utils/transform.py` reaches for
`validator_instance.resolver`, which jsonschema 4.18 deprecated in favour of `referencing`.
It warns but works, and it is what the `<5` ceiling is actually holding back.

### `pydantic` v2

`ConnectorSpecification.documentationUrl` and `.changelogUrl` were `AnyUrl`, which is a `str`
subclass under v1 but a parsing, normalising type under v2 — it appends a trailing slash to a
bare host, so `https://docsurl.com` would become `https://docsurl.com/`. Both fields are only
ever passed through to Flow's `ConnectorSpec.documentationUrl`, which is a plain `str`, so they
are `Optional[str]` here and a connector's `spec.json` reaches the UI as written.

`InternalConfig.limit` and `.page_size` were annotated `int` with a `None` default, which v1
quietly accepted. They are `Optional[int]` now. Nothing in the subset calls that class's
serializer, so its `dict()` override was renamed rather than reworked.

### `distutils`

Upstream's `sources/utils/transform.py` does `from distutils.util import strtobool`. `distutils`
was removed from the standard library in Python 3.12 and the images are built on
`python:3.14-slim`, so this worked only because `jsonschema` 3.2.0 requires `setuptools`, which
installs a `distutils` shim. Since `streams/core.py` imports that module, losing the shim would
have broken every shim connector at import time, not just boolean coercion — and relaxing the
`jsonschema` ceiling would have lost it. `strtobool` is now carried in the module itself.

## HTTP timeout

Upstream passes no timeout to `requests`, which has no default of its own, so an idle connection
can hang a capture indefinitely. `airbyte_cdk/__init__.py` patches `requests.Session.send` to
fill in a two-hour timeout whenever the caller passes none. The patch covers `requests.Session`
as a whole, not just `HttpStream._send`, because several connectors call `requests` directly.
Since it sits in the package's `__init__.py`, importing any part of `airbyte_cdk` applies it.

## Modifying this tree

This is a fork, not a mirror: edit it in place. It is not re-synced from upstream, and no new
Airbyte CDK surface should be added to it — the connectors listed in `estuary-cdk/pyproject.toml`'s
`airbyte-shim` extra are its only consumers, and the long-term direction is to replace each of them
with a native `estuary_cdk` connector.
