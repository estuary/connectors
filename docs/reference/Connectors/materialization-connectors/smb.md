---
description: Write Estuary collections as compressed CSV files to SMB shares on Windows servers, Samba, and NAS devices.
---

# CSV Files on SMB Shares

This connector materializes [delta updates](/concepts/materialization/#delta-updates)
of Estuary collections as gzip-compressed CSV files on an SMB share. It supports
Windows file shares, Samba, and NAS devices that support SMB 2 or SMB 3 and NTLM
authentication with message signing.

Use this connector when an ERP, finance application, or reporting job reads files
from a shared folder. Estuary batches rows at the configured upload interval and
writes separate `.csv.gz` files for each collection. By default, each file starts
with a header row, with collection key columns first.

## Prerequisites

* An SMB share reachable from your Estuary task's network on TCP port 445, or a
  custom port you configure. For private servers, use a deployment with network
  access to the server.
* An NTLM username and password, and a domain if your account requires one.
* Permission to create folders and to create, write, rename, replace, and delete
  files in the destination directory. Both share permissions and filesystem
  permissions must allow these operations.
* A server that supports SMB message signing, which the connector requires.

During validation, the connector creates any missing destination folders, writes
a small temporary probe file, and deletes it. Validation fails if authentication,
share access, or writing and deleting the probe fails.

## Configuration

### Endpoint

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/host`** | Host | Hostname or IP address of the SMB server. | string | Required |
| `/port` | Port | TCP port of the SMB server. | integer | `445` |
| **`/share`** | Share | Share name only, such as `data`. Do not include a server name or folder path. | string | Required |
| **`/username`** | Username | Username for NTLM authentication. | string | Required |
| **`/password`** | Password | Password for NTLM authentication. Stored as a secret. | string | Required |
| `/domain` | Domain | NTLM domain for the account, if needed. | string | Optional |
| `/directory` | Directory | Folder inside the share, such as `exports/flow`. Missing folders are created. | string | Share root |
| **`/uploadInterval`** | Upload Interval | Frequency of file uploads: `1s`, `1m`, `5m`, `15m`, `30m`, or `1h`. | string | Required; UI default `5m` |
| `/fileSizeLimit` | File Size Limit | Approximate maximum file size in bytes. Omit or set to `0` to use the default. | integer | `10737418240` (10 GiB) |
| `/csvConfig/skipHeaders` | Skip Headers | Omit the header row from each CSV file. | boolean | `false` |
| `/advanced/feature_flags` | Feature Flags | Internal options; change only as directed by Estuary support. | string | Optional |

### Bindings

| Property | Title | Description | Type | Required/Default |
|----------|-------|-------------|------|------------------|
| **`/path`** | Path | Folder for this collection under the endpoint directory, such as `orders` or `events/raw`. | string | Required |

Use forward slashes for `directory` and `path`, even for Windows servers. Both
must be clean relative paths: no leading slash, backslashes, `..`, repeated
slashes, or trailing slash. Date patterns such as `%Y` and SMB-invalid filename
characters are not supported. Leave `directory` empty to write at the share root.
Binding paths must differ by more than letter case, such as `Orders` and
`orders`, because SMB servers usually treat these as the same folder.
When editing or re-enabling a binding, keep its original path casing or
increase its backfill counter above its previous value. Validation rejects
case-only path changes at the same or a lower backfill to prevent overwriting
existing files.

### Example

```yaml
materializations:
  acme/orders-to-smb:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-smb:v1
        config:
          host: files.example.com
          share: data
          username: estuary
          password: <password>
          domain: EXAMPLE
          directory: exports/flow
          uploadInterval: 5m
    bindings:
      - resource:
          path: orders
        source: acme/orders
```

## File names and delivery

Files within the share use this layout:

```text
<directory>/<path>/v<backfill, 10 digits>/<counter, 20 digits>.csv.gz
```

For example:

```text
exports/flow/orders/v0000000000/00000000000000000000.csv.gz
exports/flow/orders/v0000000000/00000000000000000001.csv.gz
exports/flow/events/raw/v0000000000/00000000000000000000.csv.gz
```

Counters increase independently for each binding and continue from saved
connector state after a restart. A new backfill writes to a new `v` folder and
restarts its file counter. Earlier files remain on the share.

Each file is first written with a `.partial` suffix. A file appears under its
final name only after its transaction commits, so downstream jobs should match
`*.csv.gz` and ignore `*.partial`. An interrupted task may leave partial files
until it resumes; do not consume these files as completed exports. Downstream
jobs may move or delete completed `.csv.gz` files, but must leave `.partial`
files in place.

## Limitations

* Files contain delta updates. An update or delete produces a new row instead of
  changing an earlier row. Downstream consumers must apply these changes if they
  need a current-state view. Select the `/_meta/op` projection to distinguish
  delete rows.
* Only NTLM sign-in is supported. Kerberos, DFS, and SSH tunnels are not supported.
* Output is gzip-compressed CSV. Uncompressed CSV and Parquet are not supported.
* Directory and binding paths cannot contain date patterns.
* The connector does not delete old files. Manage retention separately.
* Each materialization needs its own directory, or distinct binding paths. A
  second materialization, or a deleted and recreated one, writing to the same
  directory and paths restarts file counters at 0 and replaces existing files
  with the same names.
* To change `host` or `share`, first disable the task and wait for it to stop.
  Then check that no `.partial` files remain on the old share, change the
  endpoint, and re-enable the task. A file still pending when the endpoint
  changes stays on the old share as a `.partial` file, and the connector logs a
  warning naming it.
* Each materialization runs as a single shard.
