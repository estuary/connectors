{{ define "prereq_logical_replication_short" -}}logical replication isn't enabled: current wal_level = {{.wal_level}}{{- end }}
{{ define "prereq_logical_replication" -}}
## Logical Replication Is Not Enabled

In order to capture change events from the database logical replication needs to be enabled by
setting [the `wal_level` parameter](https://www.postgresql.org/docs/current/runtime-config-wal.html)
to `logical`. Currently this parameter appears to be set to `{{.wal_level}}`. 

The [connector documentation](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/)
contains full setup instructions for various managed database providers and self-hosted databases. On self-hosted
databases this setting can be changed by running the command

```
ALTER SYSTEM SET wal_level = logical;
```

Note that a database restart is usually required for this parameter change to take effect.
{{- end }}

{{ define "prereq_table_select_short" -}}user {{.user | printf "%q"}} cannot read from table {{.table | printf "%q"}}{{- end }}
{{ define "prereq_table_select" -}}
## Table Not Readable

The capture user `{{.user}}` is not able to SELECT data from table `{{.table}}`. This
can caused by several issues, but is usually either a permissions problem or a table
which has been deleted or renamed.

Ensure that the table exists and if needed `GRANT SELECT` on that table to `{{.user}}`.
{{- end }}