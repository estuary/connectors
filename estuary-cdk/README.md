# Estuary Connector Development Kit

## Overview

The Estuary CDK is a framework and library for building connectors that integrate with Estuary's platform. 
Use it to implement connectors that capture data from various sources, such as databases and SaaS APIs.
Support for implementing derivations and materializations is coming soon.

The CDK is designed for modern Python, which means:

- It leans heavily into async Python and `asyncio`.
- It implements and recommends type annotations throughout.  
- It uses Pydantic for representing and validating data, as well as for generating the JSON schemas used by the Estuary platform.

The rough architecture of the CDK is a framework, which calls a user-developed connector, which optionally calls "common" components that solve problems common to many connectors.
The "framework" portion of the CDK tries to be as un-opinionated as possible, allowing any kind of connector use case, while common components are opinionated to reduce boilerplate and repetition across connectors.

## Canonical Layout of a Connector

A canonical connector consists of:

- A `models` module which implements Pydantic models for API interactions and output documents.
- An `api` module (and optional sub-modules) which implement pure functions that interact with an API and produce models.
  - Often these functions will ultimately implement one of the common.FetchPageFn, common.FetchSnapshotFn, or common.FetchChangesFn interfaces.  
- A `resources` module that ties together the API and models.
  - Typically, it will implement an `all_resources()` function which returns a list[common.Resource], which bind models and API functions together into a Resource concept that can be introspected and captured.

The CDK's `common` module implements common strategies for capturing data, including:

- Capturing from a logical log of changes, implemented as common.FetchChangesFn.
- Capturing from an enumerated backfill of a resource, implemented as common.FetchPageFn (which can happen concurrently with fetching incremental changes).
- Capturing a "snapshot" of a resource and emitting it if it's content has changed, implemented as common.FetchSnapshotFn.

## Why Another Connector Kit?

Good question! Within the Python ecosystem, there are Singer taps, Meltano's singer-sdk, and Airbyte's CDK.
We tried not to have to write a new one. We really did. This CDK started out as a wrapping shim around Airbyte's CDK (or Singer taps), and that's still supported today.  

But... it's awkward, for a few reasons:

- Estuary cares _a lot_ about document schema. Documents are fully validated every time they're written or read, and it's exceedingly common for third-party connectors to have incorrect schemas that fail to actually validate.

- At the same time, Estuary has a powerful continuous schema inference feature, which means that connectors often don't _need_ to care about many portions of their output schema. In fact, being overly prescriptive about a source schema that isn't functionally relevant to how the connector works is considered a bit of an anti-pattern.

- Many Estuary connectors are able to scale out, where multiple instances of a connector will jointly process a task in parallel, which is not modeled by existing connector tool kits.

- Estuary is a streaming platform. Third-party connectors are designed to run in batch contexts, where every now and then they're invoked, they spit out all new changes, and then they exit until run again. Estuary connectors run continuously, and are designed to emit changes with as little latency as possible.

- Worse, existing tool-kits are written with serial control flow: they loop over a set of bound resources, one at a time, emit all changes, and then move on to the next. In comparison, the Estuary CDK is fundamentally async at it's core. It attempts to make as much progress as possible, concurrently, against every bound resource. In practice, this dramatically increase the potential throughput of a connector, as much of the run time is typically waiting on network IO.

- Estuary has a powerful "connector networking" feature which means connectors can implement bespoke APIs that are accessible over the web. We'd like our CDK to lean into this feature.

Collectively, these fundamental differences in the scope of a connector forced our hand.
