# Feature Flags

Sometimes we need a mechanism for customizing connector behavior per-task in a somewhat
behind-the-scenes kind of way. Uses for this mechanism include:

 - Changing the default behavior of a connector while preserving the existing behavior for
   all preexisting tasks.
 - Implementing a risky, experimental, or power-user feature which we can explicitly enable
   (or direct a user to enable) on specific tasks without making it a global change.

The mechanism we've settled on for this purpose is an advanced property in the endpoint
config which holds "feature flags". This property should be a string, located at the
path `/advanced/feature_flags` in the config, which holds a comma-separated list of
flag settings.

For a given flag `do_something` there are two valid flag settings: `do_something` and
`no_do_something`. These flag settings are combined at connector startup with a list
of connector default values for the flags, so that in effect we have a ternary option
of explicit-yes / explicit-no / default for each feature flag.

## Changing Default Behaviors

The process for changing a default connector behavior is as follows:

 - Add a feature flag `new_thing` with default value false, which controls the relevant
   connector behavior via an `if featureFlags["new_thing"] {...}` at the appropriate
   point(s) in the code.
 - Edit the endpoint configs of all tasks that currently exist in production to add the
   `no_new_thing` flag setting.
 - Merge another PR which changes the connector default for `new_thing` to true.

Note that there's a sort of race condition here where any tasks created in between the
bulk config edit and the connector default change will exhibit a change in behavior. This
is probably unavoidable, since there is no way to atomically merge the default-change
simultaneously with the endpoint config edit, and doing things in the opposite order
would behave even worse (if a task got unlucky and restarted at the wrong moment,
it would pick up the default-change PR before the `no_new_thing` flag setting and
exhibit the new behavior temporarily before going back to the old behavior). The window
here is minutes at most and it only impacts new task creation, so just try to perform the
two operations close together and keep an eye on whether any new tasks were created during
the process.

## Experimental Features

An experimental feature is even simpler, just add a feature flag controlling the new
behavior. Normally this should be default-off, but if it makes things clearer there
shouldn't be anything wrong with adding a default-on flag instead and having `no_foobar`
be the setting which opts into the new behavior.

## Bulk Editing Process

There are a couple of scripts I've written which make it easier to add a feature flag
to all production tasks using a particular connector.

The `scripts/list-tasks.sh` script queries the Supabase `live_specs` table for all
tasks running a particular connector, and optionally can add them all to the active
`flowctl` draft automatically.

The `scripts/bulk-config-editor.sh` script can modify every `*.config.yaml` file
under the current directory to add a feature flag setting to the appropriate property.

The complete workflow goes something like this:

```bash
$ mkdir -p draft && cd draft && echo '*' > .gitignore          # Create a directory somewhere to hold the Flow specs being edited
$ flowctl draft create                                         # Create a new draft
$ ../scripts/list-tasks.sh --connector=source-mysql --draft    # Add all source-mysql (and variants) captures to the current draft
$ flowctl draft develop                                        # Pull down draft specs into a local directory
$ ../scripts/bulk-config-editor.sh --set_flag=do_some_thing    # Add the 'do_some_thing' feature flag to the local endpoint configs, with interactive diffs and prompting
$ flowctl draft author --source flow.yaml                      # Upload modified specs to the draft
$ flowctl draft publish                                        # Publish the uploaded specs
```

## Worked Example

TODO(wgd): Describe and add PR links when doing the MySQL `format: date` change.

1. Adding the feature flags plumbing and a feature flag.
2. Running through the commands to set no_flag.
3. Flipping the default.
