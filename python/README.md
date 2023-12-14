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

### Requirements
In order to build python connectors, you will need to install [Poetry](https://python-poetry.org/docs/#installation).

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

## OAuth
Flow supports a special type of connector configuration for sources/destinations that support OAuth. The idea is to enable users to easily grant us the required permissions to act on their behalf, without needing to generate and copy/paste API keys. Let's first look at how Flow expects the flow (😉) to work for OAuth-enabled connectors.

### The OAuth application
Before a user can grant us permissions to act on their behalf, we have to create an entity that represents Flow in the system in question -- this is called the OAuth application. Different providers have different requirements for their OAuth applications, as well as frequently requiring varying levels of validation before an application is allowed out of testing mode. But in general, an application will have the following properties:

#### Name, logo, website, tagline, etc.
  Since the OAuth application represents Flow, it must to be able to be presented with the right name and in the right clothes. 
#### Contact info
  The provider generally needs a way to contact the owner of the application for various reasons. Sometimes this needs to be verified before the application can go live.
#### Client ID
  The Client ID is a string that uniquely identifies the application in the provider's system. This ID is allowed to be published, and is used to indicate which application the user is authorizing when they start the authorize flow, among other things
#### Client Secret
  The Client Secret is a string that allows the owner of the application to perform "trusted" actions. It should never be published or exposed, as it can be used to gain access to the resources that users have granted to us (just like we do when we need that access ourselves).
#### Redirect URI
  As explained below, the OAuth flow we're using involves a redirect step to a URI we control after the user has granted us permission. Usually there are strict limits on what this URI can be (must be HTTPS, must not include `#` fragments, etc.). For testing, you'll want to make sure you include `http://localhost:{port}/`, where `port` is emitted by `flowctl raw oauth` (usually it's `16963`, though)
### Scopes
  Frequently you'll be asked to select which "scopes" the application should be allowed to ask for. Conceptually, a scope should represent either a single action that can be performed to a single resource, or some other kind of flag or permission that the application is asking the user for. 
  
  You should select only the scopes that represent the level of access to the resources we need, and no more -- each additional scope increases the level of trust we're asking of the user, as well as widening the blast radius of a security breach. 

### The authorize step
The main operative concept in our use of OAuth is that of delegation. Rather than users just _giving_ us their own personal access tokens or API keys, we are asking for their permission to act on their behalf: it's still _our_ access key/secret that is performing the action, we're just doing it under permissions that were delegated to us by other users. So, it makes sense that the first step is to ask for, and get that consent.

#### Redirect to the provider
First, the web UI (or `flowctl raw oauth`) will request a redirect URI to send the user who is requesting to initiate the OAuth flow. This URI is different for each provider, but generally contains a few key properties:
* `response_type=code` is part of the OAuth spec indicating that we're initiating an [Authorization Code flow](https://datatracker.ietf.org/doc/html/rfc6749#section-4.1).
* `client_id` is our OAuth application's client ID. Note that we do _not_ include `client_secret` here, as that would be visible to the user
* `redirect_uri` is a URL that we control where the user will be redirected after finishing the authorization grant
* `scope` represents the scopes that we're requesting. Usually this matches the scopes selected when creating the application, but conceptually it can be any subset of those scopes. Usually this is a comma- or space-separated, URL-encoded list of scopes. 
* `state`, `code_challenge`, and `code_challenge_method` are there to prevent various attacks. Conceptually, we compute a unique identifier for this particular OAuth request (`state`), and a hidden secret that we associate with that key. We then send the hash of that hidden secret along with the request, and perform various validation steps on it later to ensure that nobody is man-in-the-middling us. 
> **NOTE**: Since this step requires that the user be redirected to the provider (so that they can use their existing logged-in session, or log in as usual), all parameters to the authorize URL will be included as query-string parameters.

Here's an example authorization URL template:
```
https://airtable.com/oauth2/v1/authorize?
    response_type=code&
    client_id={{#urlencode}}{{{client_id}}}{{/urlencode}}&
    redirect_uri={{#urlencode}}{{{redirect_uri}}}{{/urlencode}}&
    state={{#urlencode}}{{{state}}}{{/urlencode}}&
    code_challenge={{#urlencode}}{{{code_challenge}}}{{/urlencode}}&
    code_challenge_method={{#urlencode}}{{{code_challenge_method}}}{{/urlencode}}&
    scope=data.records:read%20data.recordComments:read%20schema.bases:read
```

The user will now be redirected to the provider's UI for approval.

#### Redirect back to us
After the user jumps through whatever hoops the provider requires in order to provide their consent, they will be redirected back to the provided `redirect_uri` with a few important query-paremeters along for the ride.

* `code` is a single-use token that we'll use to exchange for real credentuials representing Flow's application's delegated permission to act on behalf of the user.
* `code_challenge`, `code_challenge_method`, `state` will be used to enact various security precautions.

Our web UI will send this valuable payload up to the backend, where the next step of the process will happen: exchanging the `code` for an access (and refresh) token.

### The token exchange step
Once the backend receives the information from the authorize redirect, it will attempt to exchange that for real access credentials to the system in question. In order to do that, it must construct the authorize request. The authorize request is a POST request to the authorize endpoint. It is specified along 3 dimensions:

#### The URL
Unlike GET requests where the query parameters live in the URL, since this is a POST request generally the URL will remain fixed. That being said, the `accessTokenUrlTemplate` field does have all of the template filling capabilities as any of the other templates. For example, Shopify needs the Store ID to be included as part of the domain name:
```json
{
    "accessTokenUrlTemplate": "https://{{{config.store}}}.myshopify.com/admin/oauth/access_token"
}
```

#### The Body
For providers that expect query parameters in the body, they can be specified here. For example, the Google family of connectors look like this:
```json
{
    "accessTokenBody": "{\"grant_type\": \"authorization_code\", \"client_id\": \"{{{ client_id }}}\", \"client_secret\": \"{{{ client_secret }}}\", \"redirect_uri\": \"{{{ redirect_uri }}}\", \"code\": \"{{{ code }}}\"}"
}
```

#### The Headers
Sometimes providers require that certain headers are specified. For example, sometimes query parmeters are expected to be form-encoded, in which case you can specify headers like so:
```json
{
    "accessTokenHeaders": { "content-type": "application/x-www-form-urlencoded" },
}
```

### The response
Once we successfully make the above token exchange request, we'll get back some kind of access token. This is what the connector will use to authenticate requests to the target system. Sometimes, the shape of the access token response doesn't match what we want: enter the `accessTokenResponseMap` field. This allows us to remap the access token response into our preferred format. In this contrived example, the access token response looks like this:
```json
{
    "access": "ey...==",
    "refresh": "CRYPTOGRAPHIC_RANDOM_REFRESH",
    "expires_in": 3600
}
```
The OAuth config has this:
```json
{
    "accessTokenResponseMap": {
        "access_token": "/access",
        "refresh_token": "/refresh",
        "token_expiry_date": "{{#now_plus}}{{ expires_in }}{{/now_plus}}",
    },
}
```
And the mapped access token response will look like this (where `token_expiry_date` is 1h in the future):
```json
{
    "access_token": "ey..==",
    "refresh_token": "CRYPTOGRAPHIC_RANDOM_REFRESH",
    "token_expiry_date": "2023-12-14T15:05:00Z"
}
```

### Where does it all go?
Great, we finished the OAuth flow! We initiated a request for the user to delegate access to our OAuth application, the user consented, we got a temporary authorization code, exchanged it for (somewhat more) permanent access credentials, and transformed those credentials into the shape needed for our connector. At this point, all that's left is to provide these credentials to the connector. 

By convention we have picked the field `credentials` to receive injected OAuth values. You should configure your connector's endpoint config to receive the following config in `credentials`: 
```jsonc
{
    "credentials": {
        "client_id": "...",
        "client_secret": "...",
        // All fields from the mapped access token response are included here, e.g
        "access_token": "ey..==",
        "refresh_token": "CRYPTOGRAPHIC_RANDOM_REFRESH",
        "token_expiry_date": "2023-12-14T15:05:00Z"
    }
}
```

In addition to configuring the endpoint config, for imported connectors you will also probably have to modify the config-reading logic to read OAuth data out of the `credentials` field specifically. 
> **Note:** Also, don't forget to make `credentials` a required field!

### Debugging
You can test the OAuth flow using `flowctl raw oauth`. Make sure that you have a local stack set up and running, as the intent is to emulate the production behavior as closely as possible. 

> **Note**: Make sure your terminal is set up to run the connector in question. If your `flow.yaml` is configured to use local commands, you should be inside the proper poetry shell for example.