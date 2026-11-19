---
description: Create your own capture connector. Generate Python logic to connect to Estuary's CDK and publish a custom specification via flowctl.
---

# Creating a Custom Capture

You can create your own capture connector using Estuary's specification files
and [Python CDK](./custom-connectors.md#estuarys-connector-development-kit).
This guide walks through creation and publication steps for a capture.

## Prerequisites

Before you start, you'll need:

| Prerequisite | Notes |
|---|---|
| Python 3.12 and Poetry | Requirements to work with Estuary's CDK |
| [`flowctl`](/guides/get-started-with-flowctl), Estuary's CLI | Run `flowctl auth login` to authenticate your session |
| Connector agent skills | Optional, but recommended to generate scaffolding and initial logic for your connector |

## 1. Write the connector

Build against [Estuary's CDK](./custom-connectors.md#estuarys-connector-development-kit).
The CDK's [README](https://github.com/estuary/connectors/tree/main/estuary-cdk#readme)
provides a general overview.

You may use [agent skills](https://github.com/estuary/connectors/tree/main/.claude/skills)
to help generate connector scaffolding, or follow the skills yourself as guides
on developing your connector.

The main skill to be aware of is [`create-capture-connector`](https://github.com/estuary/connectors/blob/main/.claude/skills/create-capture-connector/SKILL.md).
This bundles several related skills to build your connector:

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

   At the very least, you will need `pyproject.toml` for dependencies and
   `<YOUR-CAPTURE>/__init__.py` to give Estuary an entry point to your connector.

   Ensure `pyproject.toml` includes a dependency for Estuary's CDK:

   ```
   [tool.poetry.dependencies]
   python = "^3.12"
   estuary-cdk = { git = "https://github.com/estuary/connectors.git", subdirectory = "estuary-cdk" }
   ```

* [`configure-auth`](https://github.com/estuary/connectors/blob/main/.claude/skills/configure-auth/SKILL.md):
Wire up authentication for your capture, such as access token, OAuth, or other
credentials.

* [`add-stream`](https://github.com/estuary/connectors/blob/main/.claude/skills/add-stream/SKILL.md):
Add new data resources to capture, such as incremental or snapshot streams from
API endpoints.

See [best practices](./best-practices.md) for general guidance on connector
creation as well as details on more specific setups.

## 2. Create the capture spec

Once your connector logic is ready, you can start using the custom connector
in a capture task, similar to creating any capture configuration with flowctl.
For this, you will need to create a capture **specification** to define your
task.

This will be similar to a standard [capture spec](/concepts/captures/#specification).

One main difference is that a standard capture defines the Docker `image` that
implements the connector. A custom connector instead defines a set of
`python/files` that implement connector logic.

You will also need to define the expected _connector_ spec, not just the spec
for the capture task. Estuary references a database for this information for
standard connectors.

For example, a capture specification using a custom connector might look like:

```yaml
captures:
  # The name of the capture
  acmeCo/examples/source-hello:
    endpoint:
      # Custom connectors define Python files rather than a Docker image
      python:
        files:
          # Required files, holding dependencies and the runnable connector
          - pyproject.toml
          - source-hello/__init__.py
          # Optional files that follow the canonical Connector CDK layout
          - source-hello/api.py
          - source-hello/models.py
          - source-hello/resources.py
        # Endpoint config properties for this capture
        config:
          greeting: Howdy
          count: 3
        # The connector's specification
        spec:
          # The endpoint config schema defines available config properties
          configSchema:
            type: object
            properties:
              greeting:
                type: string
                title: Greeting
                description: Greeting of each captured document
                default: Hello
              count:
                type: integer
                title: Count
                description: Number of greetings to capture
                default: 10
                minimum: 0
    bindings: []
```

Once you have an initial specification, use the `flowctl discover` command to
discover available bindings and infer collection schemas. This will fill out
the empty `bindings` list and generate associated collection information.

Estuary will use a default spec for a custom connector's resources, including
a resource `name` and update `interval`.

### Working with secrets

Many source systems require some kind of authorization to access data. You can
manage credentials and other sensitive configuration properties by setting a
**secret** object in Estuary. This allows you to reference encrypted values
rather than working with plaintext properties in your configuration.

To reference a secret in your capture spec, create a `secrets` stanza at the
same level as the `config`:

```yaml
config:
  nonsensitiveSetting: 123
secrets:
  acmeCo/secret/path: /password
spec:
  configSchema:
    type: object
    properties:
      nonsensitiveSetting:
        type: integer
        title: Non-sensitive Setting
        description: A regular config property
      password:
        type: string
        title: Password
        description: Sensitive credentials that should be encrypted
```

In this instance, Estuary uses the encrypted value at `acmeCo/secret/path` as
the capture's `password` property. Any non-sensitive settings can remain in
the `config`.

## 3. Publish and verify

You can now publish the capture task that uses your custom connector:

```bash
flowctl catalog publish --source flow.yaml
```

Check the status of your connector and its logs with additional flowctl
commands:

```bash
flowctl catalog status your-tenant/your-prefix/source-my-capture
flowctl logs --task your-tenant/your-prefix/source-my-capture --since 10m
```

Once your capture has had time to initialize, you should see an `OK` status.
Congratulations! Your custom capture is live.
