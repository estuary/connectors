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

The script `list-tasks.sh` queries the Supabase `live_specs` table for all
tasks running a particular connector, and optionally can either pull them itself or
add them to the active `flowctl` draft automatically. It is recommended to use the
`--pull` option, which writes the task specs in a flattened directory hierarchy that
guarantees that each task can be published independently.

The script `bulk-config-editor.sh` modifies every `*.config.yaml` file under the
current directory to add a feature flag setting to the appropriate property. It
handles both plaintext and SOPS-encrypted configs, so long as you have access to
the necessary KMS keys.

The script `bulk-publish.sh` independently publishes each `flow.yaml` file defining
catalog entities, so that a validation failure when publishing one doesn't prevent
the others from being published. Optionally with the `--mark` flag, the script will
also rename each `flow.yaml` file to `flow.published.yaml` so that a second run of
the same script will only retry the ones which previously failed.

The complete workflow goes something like this:

```bash
# Fetch task specs for every source-mysql (and variants) capture in production.
$ ../scripts/list-tasks.sh --connector=source-mysql --pull --dir=./specs
# Add the 'do_some_thing' feature flag to the local endpoint configs, with interactive diffs.
$ ../scripts/bulk-config-editor.sh --set_flag=do_some_thing --dir=./specs
# Upload modified specs to the draft, renaming the local files when successful.
$ ../scripts/bulk-publish.sh --mark --dir=./specs
```

### User-Managed KMS Keys

Some users manage their own KMS keys for their task configs and merely grant our
service account (`flow-258@helpful-kingdom-273219.iam.gserviceaccount.com`) access
to the keys.

This is fairly rare, but if even one or two users do that for a particular type
of task then there will be some task specs you can't edit via SOPS as yourself.
So to make this work in the general case, you may need to impersonate `flow-258`
for the purpose of accessing these KMS keys. Assuming that you personally have
the necessary authorization for that impersonation (setting that up is outside
the scope of this bit of documentation), you can add it into the bulk editing
workflow as follows:

```bash
$ gcloud auth application-default login --impersonate-service-account \
        flow-258@helpful-kingdom-273219.iam.gserviceaccount.com
$ ../scripts/list-tasks.sh --connector=source-postgres --pull --missing=no_foobar
$ ../scripts/bulk-config-editor.sh --set_flag=no_foobar
$ ../scripts/bulk-publish.sh --mark
$ gcloud auth application-default login you@estuary.dev
```

### Failed Publications and Retrying

Some number of task edits will generally fail to publish the first time around.
There are two ways you can address this, and I recommend using first one and
then the other.

Firstly, you can simply try publishing them again. The `--mark` flag on the
bulk publishing script will rename each successfully-published task spec so
that a subsequent run of the script won't see that task any more, so if you
simply run it twice you get two tries at publishing each update:

```bash
$ ../scripts/bulk-publish.sh --mark
$ ../scripts/bulk-publish.sh --mark
```

Secondly, you can throw everything away, pull the set of tasks which still
don't have the desired flag setting, and make the edit again. This helps if
the reason for the failure is that some other publication came in and modified
the task after you pulled the specs and before you published the change:

```bash
$ rm -r specs/
$ ../scripts/list-tasks.sh --connector=source-mysql --pull --missing=no_foobar
$ ../scripts/bulk-config-editor.sh --set_flag=no_foobar
$ ../scripts/bulk-publish.sh --mark
```

Finally, some tasks may just not be able to be updated because they are in a
persistent failure state and so validation always fails. This might happen, for
instance, if the target database went offline last week and never came back. We
don't want to just skip over these tasks (because then if the DB comes back the
next day the task will exhibit an unintended change in behavior when the default
in the connector is toggled), but they can't pass validation. So what do we do?

In the long run I am told this will stop being a problem because we want automation
which will automatically disable persistently-failing tasks. It is not intended
behavior that a task can sit there failing every five minutes for weeks and then
be guaranteed to come back up without user action when the DB is fixed, that's
just how things happen to work today. So you can feel free to simply investigate
any persistently failing tasks, establish that they are indeed failing over and
over and have been for a while, and then disable them.

After disabling the offenders, you can update their flags and since validation is
not run on a disabled task the publication will go through. Note that the disable
itself was a publication though, so you will have to re-pull the specs and edit
them again:

```bash
$ rm -r specs/
$ ../scripts/list-tasks.sh --connector=source-mysql --pull --missing=no_foobar
$ ../scripts/bulk-config-editor.sh --set_flag=no_foobar
$ ../scripts/bulk-publish.sh --mark
```

## Worked Example

Here's a complete example of adding and toggling the MySQL `date_schema_format`
flag (which added `format: date` to the JSON schemas of date columns).

First I merged a PR ([source-mysql: Feature-flagged date fix](https://github.com/estuary/connectors/pull/2374/commits))
which added the feature flag with default value `false`, along with the implementation
of that change. Since this was the first `source-mysql` feature flag I also had to add
the feature flag boilerplate. These two actions are divided into two distinct commits
in the linked PR.

Second, I prepared another PR ([source-mysql: Make date_schema_format the default](https://github.com/estuary/connectors/pull/2384))
which merely toggled the connector default from `false` to `true` and updated any
tests which would demonstrate the change in default behavior. This PR was uploaded
and approved beforehand with the understanding that I would merge it at the correct
moment.

Next, I modified all ~300 production `source-mysql` (and variants) tasks to add the
explicit flag `no_date_schema_format` to each of them. As described in the section on
retrying failed publications, this took a few rounds of work but eventually every last
production task had been updated:

```bash
## Initial attempt to update everything
$ ~/work/estuary/connectors/scripts/list-tasks.sh --connector=source-mysql --pull
$ ~/work/estuary/connectors/scripts/bulk-config-editor.sh --set_flag=no_date_schema_format
$ ~/work/estuary/connectors/scripts/bulk-publish.sh --mark
## Repeat as necessary until all tasks are updated
$ rm -r specs
$ ~/work/estuary/connectors/scripts/list-tasks.sh --connector=source-mysql --pull --missing=no_date_schema_format
$ ~/work/estuary/connectors/scripts/bulk-config-editor.sh --set_flag=no_date_schema_format
$ ~/work/estuary/connectors/scripts/bulk-publish.sh --mark
```

Finally, and immediately after verifying that there were zero `source-mysql` tasks
still missing that flag setting, I merged the aforementioned default-changing PR so
that any future tasks will exhibit the new behavior from the start.

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
    var featureFlags = common.ParseFeatureFlags(config.Advanced.FeatureFlags, featureFlagDefaults)
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

## Appendix: Script Usage Notes and Error Conditions

An assortment of notes about these scripts and some edge cases that could theoretically occur.

### Configs in YAML vs JSON

Task configs in the control plane are stored as JSON. The config encryption service tells SOPS
to output JSON. But when we pull the task specs locally they get converted to YAML.

This is important to note because you actually get slightly different outputs if you ask SOPS
to emit YAML versus asking SOPS to emit JSON and then converting that to YAML yourself. For
instance, an unset property in the SOPS stanza will be represented like `kms: null` in JSON
but `kms: []` in YAML output mode. Thus if we want to minimize spurious diffs we also need to
ask SOPS to emit YAML.

That means that if you pay attention, the endpoint configs will be idiomatic YAML after running
the `list-tasks` script and be transformed into JSON after running `bulk-config-editor`. Since
YAML is a superset of JSON this is fine, and the diffs printed by that tool are in terms of the
reserialized YAML form of both the original and modified configs for this as well as other reasons.

### Publication Races and Expected Publication ID

When pulling specs with `list-tasks --pull` each task spec will have an `expectPubId` property.
This property remains unmodified through the bulk feature flag editing process, and could cause
a publication to fail if the task was modified by some other source before we finished.

This is generally the desired outcome. To resolve this you should probably just re-pull that
task's spec (you can use `list-tasks --pull --prefix=<name>` to do this easily) and modify the
latest version again.

### Non-Leaf Task Specs

The `bulk-publish` script relies on an assumption that for each task there exists a leaf `flow.yaml`
file which just defines that task and doesn't include any others. Branch files which just import
other subdirectory `flow.yaml` files are automatically ignored for publication. But it's possible
in theory to get an error that says:

    file foo/bar/flow.yaml imports other files and also contains other non-import data, which is not supported

This most likely means that there are two tasks where one's name is a prefix of the other, like:

    acmeCo/foo/bar/source-whatever
    acmeCo/foo/source-whatever

And that you didn't use `list-tasks --pull` to fetch them but instead probably used `flowctl develop`.
The fact that this is possible is one reason why the `list-tasks --pull` command exists, because it
writes its output files in a modified directory structure where each task is fully independent.

### Inlined Endpoint Configs

The way `flowctl catalog pull-specs` works is it breaks out the endpoint config (and resource
configs and whatnot) into separate files if they exceed a threshold (512 bytes, at the time of
this writing). If the endpoint config is below this threshold it will be inlined into the task
`flow.yaml` instead.

In general this doesn't happen because the SOPS stanza of an encrypted endpoint config is over
700 bytes just on its own. And most plaintext configs (of which there are a few in production)
exceed 512 bytes for other reasons.

But it's theoretically possible to have a task whose endpoint config doesn't get broken out as
a separate file. The bulk config editing script checks for this and will emit an error like:

    task dir specs/acmeCo_foobar has no corresponding config file
    FATA[0000] error        err="found 7 task directories but only 6 endpoint configs"

If this happens, the easiest solution if you want to use the bulk editing and publishing scripts
is to go modify the problematic task spec(s) by hand to break out the config. Or you could go
make the bulk editing tool smart enough to do that automatically. Or you could go improve the
task listing tool so it pulls its own task specs and fully controls how they're written to disk
instead of relying on `flowctl catalog pull-specs` to do that.