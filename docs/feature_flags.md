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

## Adding a New Flag

Add the flag's default value to the default-values map, along with a short explanatory
comment. Flags names should be in `snake_case`, and all flags should have a default value
in this map as well as a comment so that there's a single location to check and see what
flags a particular connector supports:

```go
var featureFlagDefaults = map[string]bool{
  // A short (one or two line) description of what this flag does when set/unset.
  "do_something": false,
}
```

Then at the appropriate point(s) in the code, introduce the new logic conditional on the
flag setting:

```go
if db.featureFlags["do_something"] {
  panic("do_something feature flag not yet implemented")
}
```

If this is the first feature flag added to the connector, you may need to add the
typical boilerplate as well.

## Experimental Features

For an experimental feature, just add a feature flag controlling the new behavior.
Typically this should be default-off, but if it makes things clearer there shouldn't
be anything wrong with adding a default-on flag instead and having `no_foobar` be the
setting which opts into the new behavior.

## Changing Default Behaviors

The process for changing a default connector behavior is as follows:

 - Add the feature flag `new_thing` with a default value corresponding to the old behavior (typically false).
 - Edit the endpoint configs of all tasks in production to add the `no_new_thing` flag setting.
 - Merge a followup PR which changes the connector default for `new_thing` to true.

Note that there's a sort of race condition here where any tasks created in between the
bulk config edit and the connector default change will exhibit a change in behavior. This
is unavoidable, since there is no way to atomically merge the default-change simultaneously
with the endpoint config edit, and doing things in the opposite order would behave worse if
we got unlucky with task restart timing. The window of concern here is minutes and it only
impacts new tasks, so just try to perform the two operations close together and keep an eye
on whether any new tasks were created during the process.

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
$ ../scripts/list-tasks.sh --connector=source-mysql --pull --dir=./specs     # Fetch task specs for every source-mysql (and variants) capture in production
$ ../scripts/bulk-config-editor.sh --set_flag=do_some_thing --dir=./specs    # Add the 'do_some_thing' feature flag to the local endpoint configs, with interactive diffs
$ flowctl draft author --source flow.yaml                      # Upload modified specs to the draft
$ flowctl draft publish                                        # Publish the uploaded specs
```

## Worked Example

TODO(wgd): Describe and add PR links when doing the MySQL `format: date` change.

1. Adding the feature flags plumbing and a feature flag.
2. Running through the commands to set no_flag.
3. Flipping the default.

## Appendix: Feature Flag Boilerplate

Edit `main.go` to introduce a default-settings map:

```go
var featureFlagDefaults = map[string]bool{}
```

add the feature flags property to the advanced config struct:

```go
type advancedConfig struct {
// [...other properties...]
    FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}
```

and introduce some startup logic to parse the flags property, merge with default values,
and save the parsed flag settings into the appropriate state struct.

```go
    var featureFlags = boilerplate.ParseFeatureFlags(config.Advanced.FeatureFlags, featureFlagDefaults)
    if config.Advanced.FeatureFlags != "" {
        log.WithField("flags", featureFlags).Info("parsed feature flags")
    }
    // [...a bit later...]
    db.featureFlags = featureFlags
```

Finally add a command-line flag in `main_test.go` so that flag settings can be overridden
on the command-line when running the test suite:

```go
testFeatureFlags = flag.String("feature_flags", "", "Feature flags to apply to all test captures.")
```

and plumb that command-line flag value into the capture configuration at the appropriate
point(s):

```go
captureConfig.Advanced.FeatureFlags = *testFeatureFlags
```
