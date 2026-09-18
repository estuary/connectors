---
description: Troubleshoot custom connector development against common errors you may encounter.
---

# Troubleshooting Custom Connectors

If you run into errors while developing custom connectors, use this guide to
troubleshoot and find solutions. Also check [known limitations](./custom-connectors.md#limitations),
as the custom connector feature is still in beta.

If you don't find an answer here, please [reach out](mailto:support@estuary.dev)
or [send us a Slack message](https://go.estuary.dev/slack). This will help us
help you as well as providing valuable feedback on how we can further improve
this beta feature.

## Common Errors

Almost everything fails at publish time rather than at run time. An architecture mismatch fails atomically, so no task or collection is created and there is nothing to clean up.

| Error | Stage | Reason | Solution |
|---|---|---|---|
| `ERROR: Requested estuary-cdk @ git+https://github.com/estuary/connectors.git@main#subdirectory=estuary-cdk has invalid metadata` | Local setup | The `estuary-cdk` dependency doesn't work with pip | Use Poetry rather than pip |
| `image is missing required 'FLOW_RUNTIME_PROTOCOL' label` | Publish | Dockerfile does not include which runtime protocol to use for the connector | Add `LABEL FLOW_RUNTIME_PROTOCOL=capture` to your Dockerfile; it must be a `LABEL` rather than an `ENV` |
| `image config has neither entrypoint nor cmd` | Publish | Dockerfile does not specify an entrypoint into the image | Add a `CMD` or `ENTRYPOINT` such as `CMD ["/opt/venv/bin/python", "-m", "source_my_capture"]` |
| `invalid connector image name '...'` | Publish | The reference needs a `:tag` or an `@sha256:` digest | Specify a tag when building your connector image |
| `WARNING: image platform (linux/arm64) does not match the expected platform (linux/amd64)` | Publish | [Incorrect image platform](./create-capture.md#setting-the-platform-flag) used during build | Rebuild the image with `--platform linux/amd64` |
| `manifest unknown` or `unauthorized` | Publish | The package is private | Ensure your [package is public](./create-capture.md#3-push-the-image-and-make-the-package-public) |
| `connector image '...' is unknown to Estuary, so the endpoint configuration cannot be encrypted` | Publish | Estuary's runtime doesn't have all the details to encrypt custom connector configs | Ensure you have [pre-encrypted the config](./create-capture.md#4-generate-the-spec-and-encrypt-the-config) |
| `connector image '...' is not allowed in public data planes` | Publish | The task landed on a public plane | Ensure the `--init-data-plane` flag is set to a private or BYOC data plane when you use `flowctl catalog publish` |
| `connector returned an unexpected protocol version` | Publish | Your CDK version is out of step with the runtime | Try re-installing dependencies/rebuilding your image without a cache to use the latest dependency versions |
| Data Preview reads "Not Found" | Running | First connector poll may not have run yet | Try [backdating the initial cursor](./create-capture.md#start-your-cursor-in-the-past) |
| Networking errors | Running | Data planes are dual-stack and a connector may egress over IPv6 even when IPv4 is available; you may have only allowlisted one IP family | If you allowlist Estuary's egress addresses on your side, allowlist both families |

Failed publishes leave orphaned drafts behind. These are harmless but will accumulate as you iterate.
