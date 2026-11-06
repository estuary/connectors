# D O C S

This directory contains two kinds of documentation:

## Engineering docs (this directory)

Internal references for connector developers.

- [Captures](capture/README.md) — capture gRPC protocol reference
- [Materializations](materialize/README.md) — materialization gRPC protocol reference
- [Glossary](glossary.md) — common terms
- [Inbound networking](inbound_networking.md) — Dockerfile `LABEL`s for exposed ports
- [Feature flags](feature_flags.md) — per-task feature-flag mechanism + bulk-publish workflow
- [OAuth](oauth.md) — `OAuth2Spec` template fields and the authorize/token-exchange flow

## Customer-facing docs (`reference/Connectors/`)

Markdown sources for the connector pages published to
[docs.estuary.dev](https://docs.estuary.dev). These live alongside the
connector code so that a single PR can change both behavior and docs.

The directory layout mirrors the public URL exactly, e.g.

```
docs/reference/Connectors/capture-connectors/PostgreSQL/
  → docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/
```

The Docusaurus site that hosts these pages lives in
[`estuary/docs`](https://github.com/estuary/docs), which sources this
directory via git submodule.

### Deprecating a connector page

When a newer connector replaces an old one that customers still run, keep
the old page and mark it deprecated:

1. Add a `:::deprecated` admonition near the top that links to the
   replacement connector.
2. Add `(deprecated)` to the H1 and to the front matter `title:`. Without
   `title:`, the sidebar shows the file name.
3. Set the front matter `description:` to say the connector is deprecated
   and name the replacement.
4. Add this block below the front matter. It removes the page from search
   results and from `sitemap.xml`, and the URL keeps working. Front matter
   `noindex: true` does nothing.

   ```html
   <head>
     <meta name="robots" content="noindex, follow" />
   </head>
   ```

5. Add `(deprecated)` to the connector's title in the web app (the `title`
   column of the control-plane `connectors` table).
6. When no paying customer has an enabled task on the connector, remove the
   page as the next section describes. Ask the Solutions team to check usage.

### Removing or renaming a connector page

The docs site is rebuilt from this directory, so deleting a `.md` file here
removes the page from the site. In the **same PR** as the removal or rename:

1. Add a redirect in [`redirects.yaml`](redirects.yaml). The site build reads
   that file and emits a redirect from the old URL to whatever you point it
   at (a replacement connector, or the connector category index).
2. Fix links to the old page. Search this directory and
   [`estuary/docs`](https://github.com/estuary/docs) for the file name and
   the URL path, then update or remove each link. The category indexes
   (`capture-connectors/README.md`, `materialization-connectors/README.md`)
   and sibling pages that point to each other often link to it.

   ```sh
   grep -rnE 'old-page(\.md|/)' docs/reference
   ```

