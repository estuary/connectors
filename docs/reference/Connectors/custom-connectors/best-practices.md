---
description: Best practices for custom connector development. Avoid pitfalls and learn more about working with Estuary's CDK.
---

# Best Practices

This guide collects best practices to keep in mind while developing custom
connectors. Avoid pitfalls and ensure your connectors work efficiently in
Estuary's ecosystem.

## General Guidance

### Use Poetry as your package manager

`estuary-cdk` is not published to PyPI, so you depend on it by git subdirectory.
The CDK also declares a Poetry-flavored version constraint that is not valid
PEP 508. Make sure to use Poetry rather than pip for package management.

Attempting to use pip will result in an error:

```
`ERROR: Requested estuary-cdk @ git+https://github.com/estuary/connectors.git@main#subdirectory=estuary-cdk has invalid metadata`
```

## Custom Captures

### Start your cursor in the past

A capture whose initial cursor is `now` emits nothing for a full poll interval while still reporting healthy. The CDK sees that `lag` is less than `interval` and sleeps:

```
incremental task ran recently, sleeping until `interval` has fully elapsed   sleep_for: PT4M59.9S
```

Backdate the initial cursor by one interval so that the first poll fires immediately.

:::note
The CDK gates `interval` back-off on the cursor's type. An `int` cursor silently disables the interval entirely, so any poll that checkpoints will immediately re-poll. Use a `datetime` cursor.
:::
