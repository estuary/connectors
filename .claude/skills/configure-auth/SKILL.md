---
name: configure-auth
description: Replace the AUTH SEAM left by scaffold-connector with a real authentication scheme — determine the provider's auth method, pick the matching estuary-cdk credentials primitive, wire the discriminated credentials union into models.py, define an OAuth2Spec when offering managed OAuth, set the TokenSource, and implement validate_credentials against a real probe endpoint. Use as the second step of create-capture-connector (after scaffold-connector), or any time a connector's auth needs to be (re)wired.
argument-hint: "[connector-name]"
allowed-tools: Bash Read Write Edit Glob Grep WebFetch WebSearch
---

Wire authentication into `source-$1`, replacing the `# AUTH SEAM` stubs that `scaffold-connector` left in `models.py`, `resources.py`, and `config.yaml`. The deliverable is a connector whose `flowctl raw spec` advertises the right credential options (and OAuth block, if any) and whose `validate_credentials` probes a real endpoint — **but not a live-verified one**: confirming the probe actually authenticates requires real credentials and is deferred to `bruno-probe-endpoint`.

This skill runs **as its own subagent** so the orchestrator's context stays clean. Its final message is a structured summary (see Output). The CDK auth primitives live in `estuary-cdk/estuary_cdk/flow.py` (`AccessToken`, `BasicAuth`, `OAuth2Spec`, `BaseOAuth2Credentials`, `LongLivedClientCredentialsOAuth2Credentials`, `RotatingOAuth2Credentials`, `ClientCredentialsOAuth2Credentials`, `GoogleServiceAccount`) and `estuary-cdk/estuary_cdk/http.py` (`TokenSource`). Read them before editing.

## Guiding rules

- **Always use `Field(discriminator="credentials_title")` on the `credentials` field**, even for a single scheme. This is the universal idiom and makes adding a second auth method later a non-breaking change. Each arm of the union must carry a **distinct** `credentials_title` Literal.
- **Verify against a live reference, don't trust this doc's templates blindly.** The CDK drifts; before emitting code, read the cited reference connector and reconcile. Cite the `file:line` you copied from and call out any deviation (the `scaffold-connector`/`add-stream` house style).
- **`flowctl raw spec` is free** (no API) — it's the smoke test here. Do **not** run a live `validate` or any authenticated request; there are no real credentials yet, and live checking belongs to `bruno-probe-endpoint`.
- **Don't fabricate credentials.** Leave `config.yaml` as a placeholder shaped to match the chosen scheme; real (sops-encrypted) credentials are the user's to supply.

## Phase 0 — Reconnoiter the seam

- Confirm `scaffold-connector` ran: `grep -rn "AUTH SEAM" source-$1/`. You should find the markers in `models.py` (import + `credentials` field), `resources.py` (`validate_credentials` body + `token_source`), and `config.yaml`.
- Read the current `source-$1/$PKG/{models.py,resources.py,__init__.py}` and `config.yaml`.
- Identify the provider and its base API URL (from `api.py`'s `API` constant or the orchestrator's brief).

## Phase 1 — Determine the provider's auth scheme

Research the provider's authentication docs (WebFetch/WebSearch the provider's "Authentication" / "OAuth" / "API keys" pages). Answer concretely:

- **Static credential or OAuth?** Does the provider issue a long-lived API key/token, require OAuth2, or support both?
- **If static:** how is it sent on the wire? `Authorization: Bearer <token>`, HTTP Basic (key as username), or a **custom header** (e.g. `X-Api-Key`, `Klaviyo-API-Key`)?
- **If OAuth2:** which grant? authorization-code-with-refresh-token, authorization-code-returning-a-long-lived-token (no refresh), rotating refresh tokens, or client-credentials? What are the authorize/token URLs and the token response shape?

Map the answer to a CDK primitive. In order of preference:

| Provider auth                                               | CDK primitive                                                                                         | Reference                                                   |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| OAuth2 returning a **long-lived access token** (no refresh) | `LongLivedClientCredentialsOAuth2Credentials.for_provider(...)` + `OAuth2Spec`                        | `source-intercom-native/source_intercom_native/models.py`   |
| OAuth2 authorization-code returning a **refresh_token**     | `BaseOAuth2Credentials.for_provider(...)` + `OAuth2Spec`                                              | `source-hubspot-native/source_hubspot_native/models.py`     |
| OAuth2 with **rotating** refresh tokens                     | `RotatingOAuth2Credentials.for_provider(...)` + `OAuth2Spec`                                          | `source-zendesk-support-native`                             |
| OAuth2 client-credentials (no user)                         | `ClientCredentialsOAuth2Credentials.for_provider(...)` + `OAuth2Spec`                                 | grep `ClientCredentialsOAuth2Credentials`                   |
| Bearer token / API key in `Authorization: Bearer`           | subclass of `AccessToken` (rename `credentials_title` + `access_token` title)                         | `source-klaviyo-native/source_klaviyo_native/models.py`     |
| API key sent as HTTP Basic (key=username, blank password)   | subclass of `BasicAuth`                                                                               | `source-chargebee-native/source_chargebee_native/models.py` |
| Custom header / non-Bearer scheme                           | `AccessToken` subclass **+** set `authorization_header` / `authorization_token_type` on `TokenSource` | `source-klaviyo-native` (`authorization_token_type=`)       |

**Default to offering both OAuth and a static key when both are viable** — the prevailing production shape is `OAuth2Credentials | AccessToken` (hubspot-native, intercom). The static arm keeps the connector usable even where managed OAuth isn't set up.

> **Managed-OAuth dependency (surface this!):** `OAuth2Spec.provider` must match an OAuth app **registered on the Estuary side** (the dashboard holds the client_id/secret). If Estuary has no app registered for this provider, the OAuth arm won't work end-to-end in production even though `spec` advertises it — only the static-key arm will. Call this out at the Phase 2 checkpoint as a dependency the user/ops must satisfy; it is not something this skill can resolve in code.

## Phase 2 — Checkpoint

Present to the user: the recommended scheme(s), the exact union shape (e.g. `OAuth2Credentials | AccessToken`), the probe endpoint you'll use in `validate_credentials`, and — if OAuth — the managed-OAuth registration dependency. Stop until they confirm. Auth shape and whether to offer OAuth are decisions with downstream cost; don't write code until this is settled.

## Phase 3 — Wire `models.py`

Replace the `# AUTH SEAM` import and `credentials` field. Patterns by scheme (read the cited reference and reconcile before pasting):

**Static bearer token** (rename idiom):

```python
from typing import Literal
from estuary_cdk.flow import AccessToken


class ApiKey(AccessToken):
    credentials_title: Literal["API Key"] = Field(
        default="API Key",
        json_schema_extra={"type": "string", "order": 0},
    )
    access_token: str = Field(
        title="API Key",
        json_schema_extra={"secret": True, "order": 1},
    )


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(...)  # unchanged from scaffold
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
```

**API key as HTTP Basic** — subclass `BasicAuth`, aliasing the key onto `username` and forcing a blank `password` (see `source-chargebee-native` `models.py`).

**OAuth2 (+ optional static arm)** — define a module-level `OAUTH2_SPEC`, build the credentials class via `.for_provider(...)` under a `TYPE_CHECKING` guard (the dynamic type defeats static checkers), and union it:

```python
import urllib.parse
from typing import TYPE_CHECKING
from estuary_cdk.flow import AccessToken, BaseOAuth2Credentials, OAuth2Spec

OAUTH2_SPEC = OAuth2Spec(
    provider="<provider>",
    authUrlTemplate=(
        "https://<provider>/oauth/authorize?"
        r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&response_type=code&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
    ),
    accessTokenUrlTemplate="https://<provider>/oauth/token",
    accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
    accessTokenBody=(
        "grant_type=authorization_code"
        r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
        r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
        r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
        r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
    ),
    accessTokenResponseMap={"refresh_token": "/refresh_token"},
)

if TYPE_CHECKING:
    OAuth2Credentials = BaseOAuth2Credentials
else:
    OAuth2Credentials = BaseOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(...)  # unchanged from scaffold
    credentials: OAuth2Credentials | AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
```

Copy the `authUrlTemplate`/`accessTokenBody` mustache shapes from the closest reference and adjust per the provider's docs — these are provider-specific and easy to get subtly wrong. Confirm the `accessTokenResponseMap` matches the provider's token response (e.g. `/refresh_token` vs `/access_token` for long-lived flows).

## Phase 4 — Wire `spec()` and `resources.py`

**`__init__.py`** — if you defined an `OAUTH2_SPEC`, add `oauth2=OAUTH2_SPEC` to the `ConnectorSpec(...)` returned by `spec()` (import it from `.models`). Static-only schemes leave `spec()` untouched.

**`resources.py`** — replace the seam:

- Set the token source wherever the connector authenticates (in `validate_credentials` and in `all_resources`):
  - Static: `TokenSource(oauth_spec=None, credentials=config.credentials)`.
  - OAuth: `TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)`.
  - Custom header/scheme: add `authorization_header="..."` and/or `authorization_token_type="..."` (omit defaults to `Authorization` / `Bearer`).
  - Google: add `google_spec=GOOGLE_SPEC`.
- Implement `validate_credentials` to hit a **cheap, always-available, read-only** endpoint (e.g. `/me`, `/account`, a 1-item list) and raise `ValidationError` with an actionable message on `401`. Model it on `source-front`'s `validate_credentials` (probe + `HTTPError` → `ValidationError`). Pick the probe endpoint from the provider docs; note it in the Output so `bruno-probe-endpoint` can confirm it live.

**`config.yaml`** — reshape the placeholder to the chosen scheme (e.g. `credentials: {credentials_title: "API Key", access_token: PLACEHOLDER}` or the OAuth/refresh-token fields), still with placeholder values.

## Phase 5 — Smoke test

```bash
poetry run flowctl raw spec --source test.flow.yaml -o json --emit-raw
```

Confirm the emitted `configSchema` shows the credential option(s) with the right titles and `secret` annotations, and that an `oauth2` block is present iff you defined one. `flowctl raw discover` should still succeed with zero bindings. Do not run `validate` or `preview` (no real creds; live auth is `bruno-probe-endpoint`'s job). Fix any Pydantic/import error before handing off — `for_provider` and `TYPE_CHECKING` mistakes surface here.

## Phase 6 — Hand off for credential entry & encryption

Configuration setup ends here, but **nothing downstream can run against the live API until the user supplies real credentials.** `config.yaml` still holds placeholders. Stop and hand back to the user (or, under `create-capture-connector`, signal the orchestrator to pause) with explicit instructions to:

1. Populate `config.yaml` with their real credential values for the chosen scheme.
2. Encrypt it with sops (the repo's standard — match a sibling connector's `sops`/KMS setup; e.g. `sops --encrypt --in-place config.yaml` with the project's key).

Do not proceed to `bruno-probe-endpoint` or any authenticated step until the user confirms `config.yaml` is populated and encrypted. This pause is mandatory — the credentials are the user's to provide and this skill must never fabricate or encrypt them on their behalf.

## Output

```
configure-auth: source-$1
- scheme: <AccessToken subclass | BasicAuth subclass | OAuth2 (grant) | union: OAuth2Credentials | AccessToken | ...>
- credentials_title arms: <list of distinct Literals>
- OAuth2Spec: <none | provider="<x>", token URL, response map>  (+ managed-OAuth registration dependency: <needed? >)
- TokenSource: oauth_spec=<None|OAUTH2_SPEC>, authorization_header/token_type=<defaults|custom>
- validate_credentials probe endpoint: <path>  (UNVERIFIED — confirm via bruno-probe-endpoint)
- spec smoke test: PASS/FAIL   (oauth2 block present: yes/no)
- AUTH SEAM markers removed from: models.py, resources.py, config.yaml
- BLOCKING HANDOFF: user must populate + sops-encrypt config.yaml before anything live runs (Phase 6).
- NEXT (after credentials are in): bruno-probe-endpoint to confirm the probe authenticates; then classify-stream-types / add-stream per stream.
```

## Out of scope

- Live credential verification and any authenticated request → `bruno-probe-endpoint`.
- Registering the OAuth app on the Estuary side → ops/dashboard; this skill only emits the `OAuth2Spec` and flags the dependency.
- Streams, fetch functions, pagination → `classify-stream-types` + `add-stream`.
