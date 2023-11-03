# tap-criteo

`tap-criteo` is a Singer tap for Criteo.

Built with the Meltano [SDK](https://gitlab.com/meltano/sdk) for Singer Taps.

## Installation

```bash
pipx install git+https://github.com/edgarrmondragon/tap-criteo.git
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-criteo --about
```

### Source Authentication and Authorization



## Usage

You can easily run `tap-criteo` by itself or in a pipeline using [Meltano](www.meltano.com).

### Executing the Tap Directly

```bash
tap-criteo --version
tap-criteo --help
tap-criteo --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_criteo/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-criteo` CLI interface directly using `poetry run`:

```bash
poetry run tap-criteo --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-criteo
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-criteo --version
# OR run a test `elt` pipeline:
meltano elt tap-criteo target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
