## Getting started
This module contains the building blocks to build new Flow connectors in Python, as well as adapting external Python connectors written in standard protocols to Flow. A Python connector is a subclass of the `Connector` classes in the `capture`, `derive`, and `materialize` (once it's written) submodules. Convenience classes `shim_airbyte_cdk.CaptureShim` and `shim_singer_sdk.CaptureShim` are provided to adapt existing Airbyte or Singer/Meltano connectors.

A Python connector is composed of a few critical pieces:
* `pyproject.toml` defines the Python module and its dependencies. For example:
  ```toml
  [tool.poetry.dependencies]
  # If we're pulling in an open-source connector that doesn't need any changes, we can define it here. Subdirectory can be omitted if the connector lives at the root of the repo.
  source_bigdata = { git = "https://github.com/big/data.git", subdirectory = "connector" }
  # If we're working with an open-source connector that we have pulled in-tree, we can define it like this, where the connector was pulled into the subdirectory "source_bigdata".
  source_bigdata = { path = "source_bigdata" }

  # If you're working on an Airbyte connector, you need the Airbyte CDK
  airbyte-cdk = "^0.52"
  # If you're working on a Singer/Meltaon connector, you need the singer SDK
  singer-sdk = "^0.33.0"
  ```
  > **Note:** We need to include the imported connector as a dependency rather than just referencing it as a module because it contains its own `pyproject.toml` or `setup.py`, which define any dependencies and build instructions it might have. This way, we ensure those dependencies are installed and the build instructions are followed.
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

## Pulling an open-source connector in tree
While using existing builds of open-source connectors is all well and good, there will come a time in every connector's life that we need to make changes to it. Before deciding to pull a connector in-tree, first make sure that the version of the connector you're going to pull is licensed to allow this usage, and that you have its location in the origin repository. Then, the connector can be imported like so

```bash
$ git remote add -f --no-tags bigdata https://github.com/big/data.git 
$ ./python/pull_upstream.sh bigdata master upstream/path/to/source-foobar ./source-bigdata license_type path/to/LICENSE
```

> **Note:** `license_type` here corresponds to an SPDX license identifier. You can find the list of valid licenses [here](https://spdx.org/licenses/).

> **Note:** `pull_upstream.sh` supports refs and commit hashes. The following is equally valid:
>  ```bash
>  $ ./python/pull_upstream.sh bigdata b38c2a5f upstream/path ./source-bigdata license_type path/to/LICENSE
>  ```

This will create a special merge commit that indicates the SHA of the latest commit that is being imported, where it came from, where it's going, what the specified license type was, and where that license lived. This commit format serves two purposes. First, it acts as a record of the point in time that the open source connector was imported. This is important in order to verify that the imported connector was properly licensed at the time it was imported. Second, it allows for subsequent `pull_upstream.sh` invocations to cleanly merge in upstream changes, if desired.

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
Once you've written and tested your connector, the next step is to package it into a Docker image for publication. You can build your connector with `build-local.sh <your-connector> python/Dockerfile`. This will build the connector using the Dockerfile template provided, and tag it so that `flowctl` can find your local build. If you need to customize the Dockerfile, simply omit the parameter to `build-local.sh` and the `Dockerfile` inside your connector directory will be used instead.
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

### Encrypting test credentials
In order to support rapid connector development, we would like to include encrypted credentials alongside each connector wherever feasible. This allows both easily automated testing, as well as allowing other people to quickly run all connectors that have credentials. Fortunately, Flow has built-in support for encrypted credentials through the use of [`sops`](https://github.com/getsops/sops).

Instead of defining connector configuration in `test.flow.yaml`, the `config` field can also take a filename containing an optionally `sops`-encrypted file. To create one from scratch:
1. Create a new `connector_config.yaml`
  ```yaml
  client_id: exctatic_emu@service-accounts.estuary.dev
  client_secret_sops: super_secret_password
  ```
  > **Note**: the `_sops` suffix for encrypted field is convention here. Whatever you pick for the encrypted suffix, Flow will strip that suffix out of the decrypted config object to provide to the connector. 
2. Run `sops` and overwrite the file you just created with the encrypted version:
  ``` bash
  $ sops --encrypt --input-type yaml --output-type yaml --gcp-kms projects/helpful-kingdom-273219/locations/us-central1/keyRings/dev/cryptoKeys/CI-estuary-flow --encrypted-suffix _sops path/to/connector_config.yaml
  ```
  ```yaml
  client_id: exctatic_emu@service-accounts.estuary.dev
client_secret_sops: ENC[AES256_GCM,data:c3BEsuHJLjIt7+G1hwb6x29BU7CK,iv:6LfUthR8c5DFTmucFC5NnMiOGal7v+PYixadovIm2gw=,tag:uHtTuPXLxEi4HLunVeLjkQ==,type:str]
  sops:
    kms: []
    gcp_kms:
        - resource_id: projects/helpful-kingdom-273219/locations/us-central1/keyRings/dev/cryptoKeys/CI-estuary-flow
          created_at: "2023-11-06T22:16:37Z"
          enc: CiQAW8BC2JnhfMjWVLeRYPPQgnzBVM2MtLMlh/84pcfCRbQExBcSSQBgR/fKuXztEtnXLcNceSt9XGDi0A/9nqYQrFFqTD5d0R2HEATmH4Fyqg/Gn5/sYAdDegI0g3hHYZd91rJir0TaljFQ2YRAnYw=
    azure_kv: []
    hc_vault: []
    age: []
    lastmodified: "2023-11-06T22:16:37Z"
    mac: ENC[AES256_GCM,data:LzU+fTji6MHPFjXMNqnQAizwL3jBCvt9zltFz291u81ocIMSkdFiff+KRoTHz8kYvdoBVfJt8CesdCOqGiTgLKmea7teKiJuK5bBEOuzEY4lfC1fRYVkX+Dw6t1Mx5CvlLlS3ioisVtPG53eAGMcZDhZ7iJt7nm7qvo3Tkq7pSU=,iv:1OI2BJIyxo8DNnJmGn4lqU8TlFcdG9R9GhognGyyNY8=,tag:fPjklOLlS7OzDFszFK75hg==,type:str]
    pgp: []
    encrypted_suffix: _sops
    version: 3.7.3
  ```
3. From here on, you must use sops to edit this encrypted file. Even if you only change an unencrypted field, the `mac` will no longer be valid and the file will fail to decrypt. To edit the file using your terminal's built-in editor, simply run `sops path/to/connector_config.yaml`, make changes, save, and `sops` will re-encrypt the file for you.

### Adding your connector to CI
Once you have tested your connector locally including building/running a local image, you will want to configure CI to automatically build/release changes to the connector automatically.

1. Add your connector to the matrix build in `.github/workflows/ci.yaml`
  ```yaml
  jobs:
    build_connectors:
      strategy:
        matrix:
          connector:
            - your-connector-here
  ```
2. Configure your connector. Python connectors have a slightly different build process than non-python connectors, as such you need to provide a couple pieces of metadata into the matrix job that builds your connector. 
  ```yaml
  jobs:
    build_connectors:
      strategy:
        matrix:
          include:
            - connector: your-connector-here
              python: true
              # allowed values: capture, materialization
              connector_type: capture
  ```