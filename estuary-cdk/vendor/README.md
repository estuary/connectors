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
in here because it is the source of the `pydantic<2` constraint, and because `airbyte_cdk.models`
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

`airbyte_protocol/models/airbyte_protocol.py` is left byte-identical to upstream. It is
datamodel-codegen output, and keeping it pristine makes regenerating it for pydantic v2
a mechanical step later.

## Dependencies

The subset needs `backoff`, `dpath`, `jsonref`, `jsonschema`, `pendulum`, `pydantic`, `PyYAML`,
`requests` and `requests-cache`.

`airbyte-cdk` 0.52.10 declares seventeen runtime requirements; the subset needs nine of them.
The eight it drops are `airbyte-protocol-models` (vendored here), `cachetools`, `Deprecated`,
`genson`, `isodate`, `Jinja2`, `python-dateutil` and `wcmatch` — and with `wcmatch` goes its
own dependency `bracex`.

Six of those leave every connector lock file outright. `cachetools` and `python-dateutil`
remain wherever `google-auth` or `pendulum` still require them. `Jinja2` leaves eleven of the
fourteen; the three that keep it (`source-jira-legacy`, `source-linkedin-ads-v2`,
`source-zendesk-support`) declare it directly as a CVE floor, and nothing in their graphs
needs it any more, so those pins are now vestigial.

Version constraints in `estuary-cdk/pyproject.toml` are carried over verbatim from
`airbyte-cdk` 0.52.10, so vendoring on its own does not change dependency resolution.
Relaxing them is deliberately a separate step.

### `distutils`

Upstream's `sources/utils/transform.py` does `from distutils.util import strtobool`. `distutils`
was removed from the standard library in Python 3.12 and the images are built on
`python:3.12-slim`, so this worked only because `jsonschema` 3.2.0 requires `setuptools`, which
installs a `distutils` shim. Since `streams/core.py` imports that module, losing the shim would
have broken every shim connector at import time, not just boolean coercion — and relaxing the
`jsonschema` ceiling would lose it. `strtobool` is now carried in the module itself.

## Modifying this tree

This is a fork, not a mirror: edit it in place. It is not re-synced from upstream, and no new
Airbyte CDK surface should be added to it — the connectors listed in `estuary-cdk/pyproject.toml`'s
`airbyte-shim` extra are its only consumers, and the long-term direction is to replace each of them
with a native `estuary_cdk` connector.
