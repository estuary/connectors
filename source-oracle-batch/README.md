# Oracle Batch

## Running with Flow

You'll need the latest version of flow compiled [from source](https://github.com/estuary/flow):

```shell
flowctl raw discover --flat --source test.flow.yaml
flowctl preview --source test.flow.yaml --name acmeCo/source-oracle
```

### Testing

```shell
export PYTHONBREAKPOINT=ipdb.set_trace
```

Run this within a docker container (for odbc support):

```shell
pytest -s --pdb -k test_name
```

Run full test suite with nice logging

```shell
make test
```

Tests do not yet run on CI

### Debugging

#### lptrace, gdb, pyrasite, py-spy, etc

Want to inspect a running process? Use py-spy.

 lptrace, pyrasite, gdb, etc does not work when the host architecture is different than the container architecture (ARM vs x86 architecture mismatch).

#### `rpdb`

Being restricted to a visual terminal (`debugpy`) is not ideal. There's a project which allows you to set a remote
breakpoint and then interact with it in a separate terminal running `socat` so it doesn't interfere with
stdout + stdin.



First, set your breakpoint:

```shell
export PYTHONBREAKPOINT=rpdb.set_trace
```

Run your connector. When a breakpoint is hit, in another terminal run:

```shell
bin/rpdb-connect
```

#### `debugpy`

Change your `test.flow` command:

```shell
make local-command-python-debug
```

Update your breakpoint:

```shell
export PYTHONBREAKPOINT=debugpy.breakpoint
```

Then use VS Code to attach.

#### Direct JSON Replay

It's helpful to capture the JSON being sent to flow in order to easily replay on the command line
to use `breakpoint()` and friends to debug:

```shell
jq -c . request.json | poetry run python -m source_oracle
```

Insert the following in `source_oracle.__main__` to capture all stdin json lines and output them to a file in `playground/`:

```python
from playground import patch_flow
patch_flow.install()
```

#### Inspecting Incoming JSON

In order to share the captured JSON using the flow patch above, you'll need to remove all sensitive information.
Here's a command to remove sensitive information and page through the JSON lines in a pretty printed fashion with nice headers for each individual command:

```shell
zq -Z 'drop open.capture.config,discover.config,validate.config,apply.capture.config' ./playground/incremental_request.json | ov --section-delimiter "^{" --section-header
```

Or, if you want it in JSON format:

```shell
zq -j 'drop open.capture.config,discover.config,validate.config,apply.capture.config' ./playground/incremental_request.json | jq | ov --section-delimiter "^{" --section-header
```

Inspect the state of each message on a recorded stream:

```shell
zq -Z '{...open.state}' json.lines
```
