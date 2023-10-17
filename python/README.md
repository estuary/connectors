## Getting started
This module contains the building blocks to build new Flow connectors in Python, as well as adapting external Python connectors written in standard protocols to Flow. A Python connector is a subclass of the `Connector` classes in the `capture`, `derive`, and `materialize` (once it's written) submodules. Convenience classes `shim_airbyte_cdk.CaptureShim` and `shim_singer_sdk.CaptureShim` are provided to adapt existing Airbyte or Singer/Meltano connectors.

A Python connector is composed of a few critical pieces:
* `pyproject.toml` defines the Python module and its dependencies. For example:
  ```toml
  [tool.poetry.dependencies]
  # If we're pulling in an open-source connector that doesn't need any changes, we can define it here. Subdirectory can be omitted if the connector lives at the root of the repo.
  source_bigdata = { git = "https://github.com/big/data.git", subdirectory = "connector" }
  # If we're working with an open-source connector that we have pulled in-tree, we can define it like this, where the connector was pulled into the subdirectory "upstream-connector"
  source_bigdata = { path = "upstream-connector" }
  ```
* In addition to being useful documentation for other people running your connector, `test.flow.yaml` or some other valid Flow spec is needed to invoke your connector locally. You'll point `flowctl` at this file to test your connector when developing locally.
* `__main__.py` is the entrypoint of your connector. In the case that your connector is using a shim to wrap a supported open-source connector protocol, the file will be quite simple:
  ```python
  from python import shim_airbyte_cdk
  from source_bigdata import SourceBigData

  shim_airbyte_cdk.CaptureShim(
      delegate=SourceBigData(),
      oauth2=None
  ).main()
  ``` 
* A `Dockerfile` to build your connector into a container image that will be run in production. An example dockerfile can be found in this directory. Make sure to `source-changeme` with the name of your connector. If you are writing a materialization connector, make sure to also change the `LABEL FLOW_RUNTIME_PROTOCOL` to `materialization`.

## Pulling an open-source connector in tree
While using existing builds of open-source connectors is all well and good, there will come a time in every connector's life that we need to make changes to it. Before deciding to pull a connector in-tree, first make sure that the version of the connector you're going to pull is licensed to allow this usage. Then, the connector can be imported like so

```bash
$ git remote add -f --no-tags bigdata https://github.com/big/data.git 
$ ./python/pull_upstream.sh bigdata/master path/in/source/repo path/in/this/repo
```

This will create a special merge commit that indicates the SHA of the latest commit that is being imported, where it came from, and where it's going. This commit format serves two purposes. First, it acts as a record of the point in time that the open source connector was imported. This is important in order to verify that the imported connector was properly licensed at the time it was imported. Second, it allows for subsequent `pull_upstream.sh` invocations to cleanly merge in upstream changes, if desired.

## Running/Debugging connectors
Connectors can be invoked locally using [`flowctl`](https://docs.estuary.dev/getting-started/installation/#get-started-with-the-flow-cli). For example, to inspect the `spec` output of your connector with a minimal `test.flow.yaml` file that looks like the following 
```yaml
--
captures:
  acmeCo/source-bigdata:
    endpoint:
      local:
        command:
          - python
          - "-m"
          - source-bigdata
        config:
          region: antarctica
    bindings:
      - resource:
          stream: bigdata
          syncMode: full_refresh
        target: acmeCo/big
collections:
  acmeCo/bigdata:
    schema:
      type: object
      properties:
        _meta:
          type: object
          properties:
            row_id:
              type: integer
          required:
            - row_id
    key:
      - /_meta/row_id
```
You might get some output that looks like this
```console
js$ ./python/activate.sh source-bigdata
Installing dependencies from lock file

Package operations: 3 installs, 2 updates, 0 removals
js$ poetry shell
Spawning shell within /Users/js/Documents/estuary/connectors/.venv
js$ flowctl raw spec --source source-bigdata/test.flow.yaml --capture acmeCo/source-bigdata | jq
{
  "configSchema": {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Big Data Spec",
    "type": "object",
    "required": [
      "data_name"
    ],
    "properties": {
      "data_name": {
        "type": "string",
        "description": "Name requested from the API.",
        "pattern": "^[a-z0-9_\\-]+$"
      }
    }
  },
  "resourceConfigSchema": {
    "type": "object",
    "properties": {
      "stream": {
        "type": "string"
      },
      "syncMode": {
        "enum": [
          "incremental",
          "full_refresh"
        ]
      },
      "namespace": {
        "type": "string"
      },
      "cursorField": {
        "type": "array",
        "items": {
          "type": "string"
        }
      }
    },
    "required": [
      "stream",
      "syncMode"
    ]
  },
  "documentationUrl": "https://docs.bigdata.com/integrations/sources/bigdata"
}
```

### Debugging
Since `flowctl` is ultimately just invoking the endpoint's command to run the connector, we can inject a debugger into the python startup process that will allow us to inspect the connector as it's running. For example, [debugpy](https://github.com/microsoft/debugpy) allows for network debugging, which is important because connectors are not designed to run interactively, as would be required for regular old `pdb`. For example, change your `test.flow.yaml`: 
```yaml
captures:
  acmeCo/source-pokemon:
    endpoint:
      local:
        command:
          - python
          - "-m"
          - "debugpy"
          - "--listen"
          - "0.0.0.0:5678"
          - "--wait-for-client"
          - "-m"
          - source-bigdata
```
Teach vscode how to attach to the debugger in `.vscode/launch.json`
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Remote Attach",
            "type": "python",
            "request": "attach",
            "connect": {
                "host": "localhost",
                "port": 5678
            },
            "pathMappings": [
                {
                    "localRoot": "${workspaceFolder}",
                    "remoteRoot": "."
                }
            ],
            "justMyCode": true
        }
    ]
}
```

Then run your connector, attach the debugger, and debug away!

## Building
Once you've written and tested your connector, the next step is to package it into a Docker image for publication. Make sure that you have a proper `Dockerfile` (see `python/Dockerfile.example` for a useful jumping-off point), and then run `build-local.sh <your-connector>`. This will build the connector and tag it so that `flowctl` can find your local build.
> **Hint**: `flowctl` won't try and pull Docker images tagged with `:local`, instead opting to run the locally tagged image. This is what `build-local.sh` does to ensure that your built image works when testing locally before it's pushed.

Once you've built your image, update your `test.flow.yaml` to reference it
```yaml
captures:
  acmeCo/source-bigdata:
    shards:
      # If things aren't working as expected, you can try this to get some more debug logging
      # logLevel: debug
    endpoint:
      connector:
        image: ghcr.io/estuary/source-bigdata:local
        config:
          region: antarctica
```
Then just run `flowctl raw <spec|capture|..>` like usual, and it should run your built connector!