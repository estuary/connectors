---
description: Build, publish and run your own capture connector via your own connector image on an Estuary private or BYOC data plane.
---

# Creating a Custom Capture

You can build your own capture connector as a container image and run it on
your own private data plane. This guide covers creating your own capture image
using [Estuary's Python CDK](./custom-connectors.md#estuarys-connector-development-kit).

:::warning
This feature is in **beta**.

Custom connectors are available on **private and BYOC data planes only**. They
do not run on Estuary's public cloud. See other [limitations](./custom-connectors.md#limitations).
:::

## Prerequisites

Before you start, you'll need:

| Prerequisite | Notes |
|---|---|
| Python 3.12 and Poetry | These are Estuary CDK requirements |
| [`flowctl`](/guides/get-started-with-flowctl), Estuary's CLI | Run `flowctl auth login` to authenticate |
| A [private or BYOC](/private-byoc) data plane with Estuary | Public cloud tenants cannot run custom images |
| A registry allowing **anonymous** pulls | The data plane pulls with no credentials |
| Docker | To build and push |

:::caution
Your connector image must be publicly pullable. There is no support for authenticated registries, because the runtime passes no credentials.

If your connector encodes something you cannot publish, this feature is not usable for you yet. Please [contact us](mailto:support@estuary.dev) rather than working around it.
:::

## 1. Write the connector

Build against [Estuary's CDK](./custom-connectors.md#estuarys-connector-development-kit).
The CDK's [README](https://github.com/estuary/connectors/tree/main/estuary-cdk#readme)
provides a general overview.

You may use [agent skills](https://github.com/estuary/connectors/tree/main/.claude/skills)
to help generate connector scaffolding, or follow the skills yourself as guides
on developing your connector.

These skills are particularly useful to start out:

* [`scaffold-connector`](https://github.com/estuary/connectors/blob/main/.claude/skills/scaffold-connector/SKILL.md):
Generate the main scaffolding and stub files for your capture. You will end up
with a project structure similar to:

   ```
   source-my-capture/
   ├── pyproject.toml
   ├── poetry.lock
   ├── VERSION
   ├── config.yaml
   ├── test.flow.yaml
   ├── tests/
   │   ├── __init__.py
   │   └── test_snapshots.py       # Test harness
   └── source_my_capture/
       ├── __init__.py             # Connector class (extends BaseCaptureConnector)
       ├── __main__.py             # Entry: asyncio.run(Connector().serve())
       ├── models.py               # Pydantic models for config, resources, documents
       ├── api.py                  # Pure functions for API interactions
       └── resources.py            # Binds models + API into Resource objects
   ```

* [`configure-auth`](https://github.com/estuary/connectors/blob/main/.claude/skills/configure-auth/SKILL.md):
Wire up authentication for your capture, such as access token, OAuth, or other
credentials.

* [`add-stream`](https://github.com/estuary/connectors/blob/main/.claude/skills/add-stream/SKILL.md):
Add new data resources to capture, such as incremental or snapshot streams from
API endpoints.

While custom connectors are in beta, some of these resources are still geared
towards Estuary's own use. While developing, you should keep the following in
mind:

### Use Poetry as your package manager

`estuary-cdk` is not published to PyPI, so you depend on it by git subdirectory.
The CDK also declares a Poetry-flavored version constraint that is not valid
PEP 508. Make sure to use Poetry rather than pip for package management.

If you generate your scaffolding via agent skill, the skill will create the
project's `pyproject.toml` file based off of an internally-built connector.
The CDK dependency is simply declared as a path: `path="../estuary-cdk"`.

Instead, ensure your dependency is pointed to Estuary's GitHub repo:

```toml
[tool.poetry.dependencies]
python = "^3.12"
estuary-cdk = { git = "https://github.com/estuary/connectors.git", subdirectory = "estuary-cdk" }
```

### Start your cursor in the past

A capture whose initial cursor is `now` emits nothing for a full poll interval while still reporting healthy. The CDK sees that `lag` is less than `interval` and sleeps:

```
incremental task ran recently, sleeping until `interval` has fully elapsed   sleep_for: PT4M59.9S
```

Backdate the initial cursor by one interval so that the first poll fires immediately.

:::note
The CDK gates `interval` back-off on the cursor's type. An `int` cursor silently disables the interval entirely, so any poll that checkpoints will immediately re-poll. Use a `datetime` cursor.
:::

## 2. Build the image

Write a Dockerfile for your connector image. This can be based off the
following template.

```dockerfile
# Build with Python version 3.12
FROM --platform=linux/amd64 python:3.12-slim AS builder

# git is needed because estuary-cdk installs from a git subdirectory
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Make sure Poetry is installed
RUN pip install --no-cache-dir "poetry==2.4.1"
RUN python -m venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv

WORKDIR /opt/build
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --only main

FROM --platform=linux/amd64 python:3.12-slim AS runner

# Required. Lets Estuary's runtime know to use the capture protocol.
# Must be formatted as a label.
LABEL FLOW_RUNTIME_PROTOCOL=capture

# Optional. Defaults to protobuf.
LABEL FLOW_RUNTIME_CODEC=json

COPY --from=builder /opt/venv /opt/venv
COPY source_my_capture /opt/connector/source_my_capture

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH=/opt/connector
ENV PYTHONUNBUFFERED=1

CMD ["/opt/venv/bin/python", "-m", "source_my_capture"]
```

You can then build your Docker image with the following command:

```bash
docker build --platform linux/amd64 -t ghcr.io/<owner>/source-my-capture:v1 .
```

:::note
Because the result is an index, a single push appears as three versions in your registry: one tagged and two untagged. This is the index plus its two manifests and it is expected.
:::

### Setting the platform flag

The runtime runs `linux/amd64` images only. Whether you need the `--platform` flag depends on the machine you build on rather than the one you develop on:

* If you build on Apple Silicon, an arm64 CI runner, or a Graviton host, you need it. Without it you produce an arm64 image and the publish fails.
* If you build on an x86_64 Linux host, you get an amd64 image by default and would never notice.

Pass the flag in either case. It makes the build reproducible across your team.

## 3. Push the image and make the package public

You can push your built image using:

```bash
docker push ghcr.io/<owner>/source-my-capture:v1
```

A newly pushed GHCR package is private and you must make it public by hand.
In GitHub:
* Navigate to your package
* Under **Settings**, find the **Danger Zone**
* Choose to **Change visibility** and select Public

Visibility only needs to be set once per package. Later tags on the same
package inherit it.

## 4. Generate the spec and encrypt the config

At this stage, we'll start using the custom connector in a capture task,
similar to creating any capture configuration with flowctl.

However, `flowctl catalog publish` refuses any image it does not recognize:

```
Error: connector image 'ghcr.io/<owner>/source-my-capture:v1' is unknown to Estuary,
so the endpoint configuration cannot be encrypted. Use a different connector or reach out
to Estuary support for help
```

`flowctl` looks your image up in Estuary's registry to find which config fields are marked secret. Your image is not there, so it stops.

To work around this, encrypt the config yourself so that `flowctl` skips the lookup.

### a. Write a minimal seed spec

```yaml
captures:
  your-tenant/your-prefix/source-my-capture:
    endpoint:
      connector:
        image: ghcr.io/<owner>/source-my-capture:v1
        config:
          some_setting: value
    bindings:
    - resource: { name: my_resource, interval: PT5M }
      target: your-tenant/your-prefix/my_collection
```

### b. Get the connector's config schema

This runs your image in local Docker, so it works against an unregistered image:

```bash
flowctl raw spec --source seed.flow.yaml
```

Take the `configSchema` field from the output.

:::note
`flowctl raw discover --source seed.flow.yaml` expands the seed into full bindings and inferred collection schemas. It runs `docker pull` first, so push your image before you run discover, or it will fail with `not found`.

Discover also writes your config back to disk in plaintext, including secrets. Do not run it with real credentials in the seed and do not commit the result.
:::

### c. Encrypt the config

Add a `_sops` suffix to any config field that should be secret. For example,
`api_token` should become `api_token_sops`.

Then send your complete, modified config to Estuary's encryption endpoint:

```bash
curl -X POST https://config-encryption.estuary.dev/v1/encrypt-config \
  -H 'content-type: application/json' \
  -d '{"config": {...your config...}, "schema": {...the configSchema...}}'
```

A `sops` envelope is added and any field with a `_sops` suffix is encrypted.
Non-secret fields remain in plaintext.

### d. Add the encrypted config to your spec

Use the whole returned object as your connector `config`.

You only need to follow this process once per config. Republishing with a new image tag or with spec changes will succeed without repeating it, because the presence of the `sops` envelope is what causes `flowctl` to skip the lookup. You only repeat this step when a config value changes.

## 5. Publish

You can now publish the capture task that uses your custom connector:

```bash
flowctl catalog publish --source flow.yaml \
  --init-data-plane ops/dp/private/<your-plane>
```

The `--init-data-plane` flag is important. If you omit it, the task lands on the covering prefix's default data plane. If that default is a public plane, your image is rejected even though the publish itself appeared to succeed.

Add `--auto-approve` if you are running this from a script.

## 6. Verify

```bash
flowctl catalog status your-tenant/your-prefix/source-my-capture
flowctl logs --task your-tenant/your-prefix/source-my-capture --since 10m
```

You are looking for:

```
started connector container   image: ghcr.io/<owner>/source-my-connector:v1
Capture started.
Streaming change events (all N bindings are backfilled)
created partition
```

You may also see:

* **`Unclosed client session` logged at ERROR.** Every Python CDK connector logs this on a healthy run. It is an aiohttp lifecycle artifact rather than a fault and it will be the only ERROR you see on a clean first run.
* **`all N bindings are backfilled` immediately at startup.** This means that no backfill is outstanding rather than that a backfill ran. For an incremental-only connector, it appears right away.
* **An empty stats graph in the dashboard.** On managed private deployments, the rollup that feeds dashboard stats, the OpenMetrics API and usage reporting is disabled by default and you cannot view or change the setting yourself. `flowctl logs` and `flowctl raw stats` are unaffected. [Contact us](mailto:support@estuary.dev) if you need dashboard stats enabled.

## 7. Update a connector

Estuary's own convention is to increment `v1` to `v2` only for a breaking change, to push ordinary fixes in place over the existing tag, and to tag preview builds with a commit SHA.

To ship a fix, re-push the tag and then republish the spec. `docker pull` runs on every container start, so the new image is picked up when the connector restarts and a spec change forces that restart. You do not need to repeat the encryption step.

:::caution
Task logs record only the image tag and never the digest. If you push fixes in place over a tag, `flowctl logs` cannot tell you which build is actually running. Use digest-pinned or SHA-tagged references if you need this.
:::

If you run into any errors throughout the process, see [troubleshooting](./troubleshooting.md)
for more detailed information on common issues.
