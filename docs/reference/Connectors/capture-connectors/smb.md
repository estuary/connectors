---
description: Capture files from Windows file shares, Samba servers, and NAS devices with Estuary's SMB connector. Supports subdirectories, regex filters, and incremental capture of modified files.
---

# SMB

This connector captures files from an SMB/CIFS share into an Estuary collection. It supports SMB2 and SMB3 servers, including Windows file shares, Samba, and NAS devices.

## Prerequisites

- An SMB2 or SMB3 server reachable from your Estuary data plane, or from an SSH bastion host, on the configured TCP port (445 by default). Allow connections from the [Estuary IP addresses](/reference/allow-ip-addresses) to the SMB server for direct connections, or to the bastion's SSH port when using a tunnel.
- A share and directory containing the files to capture.
- A username and password with permission to access the share, list directories, and read files. Write permission is not required.
- The NTLM domain, if your server requires one for authentication.

SMB1 and Kerberos authentication are not supported.

The connector requires SMB message signing. Guest and anonymous sessions are rejected because they cannot be signed. SMB signing does not encrypt file contents. Require SMB3 encryption with `smb encrypt = required` on Samba, or `Set-SmbShare -Name <SHARE> -EncryptData $true` on Windows. Avoid exposing port 445 directly to the internet; use an [SSH tunnel](#ssh-tunneling) or private networking.

## SSH tunneling

Set `networkTunnel.sshForwarding` to connect to an SMB share on a private network through an SSH bastion. The bastion must accept SSH connections from your Estuary data plane and allow TCP forwarding to the SMB server. See the [network connection guide](/guides/connect-network/) for bastion setup.

Set `sshEndpoint` to `ssh://user@bastion-host[:port]` (port 22 by default) and `privateKey` to the unencrypted SSH private key for that user. Install the matching public key in the bastion user's `authorized_keys`. SMB authentication still uses the separate `username`, `password`, and optional `domain` settings.

Keep `host` and `port` set to the SMB server's address as reachable from the bastion. The hostname is resolved from the bastion's network; it does not need to be publicly resolvable. For example, add this to your endpoint configuration:

```yaml
host: files.internal.example
port: 445
networkTunnel:
  sshForwarding:
    sshEndpoint: ssh://flow@bastion.example.com:22
    privateKey: |
      -----BEGIN OPENSSH PRIVATE KEY-----
      <SSH_PRIVATE_KEY>
      -----END OPENSSH PRIVATE KEY-----
```

The SSH tunnel encrypts traffic between Estuary and the bastion. Enable SMB3 encryption to protect the connection from the bastion to the SMB server as well. SMB signing remains required when using a tunnel. SMB sessions reconnect through the same tunnel; a failed SSH tunnel causes the connector to restart. Omit `networkTunnel` to keep connecting directly.

## Directories and file paths

Set `share` to the share name, such as `data`, and `directory` to a path inside that share, such as `/inbox`. Use forward slashes and a leading `/`. Use `/` to capture from the share root; other directory paths must not end in `/` or contain `.` or `..` segments. Do not include the server or share name in `directory`.

The connector recursively captures files in the configured directory and its subdirectories. The capture user must be able to read the configured directory and every subdirectory in it. A subdirectory it cannot read fails the capture, which retries and resumes once access is restored, so files in that subdirectory are not missed. The system folders `System Volume Information`, `$RECYCLE.BIN`, `DfsrPrivate` and `lost+found`, and `RECYCLER` at the share root, are skipped with a warning when they cannot be read. To leave out another folder the capture user cannot read, configure a directory which does not contain it.

When the server reports symbolic links as links (for example, Windows symlinks), links to files use the referent file's contents and modification time. File links whose target is missing, outside the share, or an absolute path such as `C:\data\file.csv` are skipped with a warning. Links to subdirectories are skipped with a warning without being resolved. Windows reports directory junctions, volume mount points and cloud-sync placeholder folders as directory links, so their files are not captured; capture the target directory instead. Servers that resolve links themselves, including Samba and most NAS devices by default, expose them as ordinary files and directories, which the connector captures as such. A directory link can capture the same files twice under different paths, and a link to a parent folder can cause a capture error. The configured directory may be, or sit under, a Windows directory symbolic link only when that link is in the share root and has a relative target. Files beneath such a directory, including file links and other reparse points such as deduplicated files, are resolved through the directory the link names; file links whose target falls outside the share are skipped with a warning.

Discovery returns one binding whose `stream` is the configured directory. Its recommended collection name is the directory with leading and trailing slashes removed, or `root` for `/`.

Each document includes these fields:

| Field | Description |
| ----- | ----------- |
| `/_meta/file` | Path relative to the share root, starting with `/`, such as `/inbox/sub/b.csv`. |
| `/_meta/offset` | Zero-based record offset within the file. |

The default collection key is `[/_meta/file, /_meta/offset]`.

## Incremental capture

The initial capture reads all matching files. Subsequent captures read new or changed files whose modification time is newer than the latest modification time previously captured. Unchanged files are skipped. When a file changes, the connector reads the entire file again, including rows that have not changed.

:::warning Copied files keep their original modification time
Windows Explorer, `copy` and `robocopy` keep the source file's modification time by default, and so does moving a file within the same volume. A file copied or moved into the share with a modification time older than the latest one already captured is skipped, with no error. Write new files directly to the share, or update their modification time after copying, for example with PowerShell: `(Get-Item <file>).LastWriteTime = Get-Date`. To capture files that were skipped this way, backfill the binding.
:::

Ensure new and updated files receive increasing modification times. Changing a file's contents without advancing its timestamp can also cause it to be skipped. When the server reports no valid modification time for a file, the connector uses the file's change time instead. If that is not valid either, the connector uses the file's creation time and logs a warning: later changes to the file are not captured. If none of these is valid, the connector logs a warning and the file is captured only by the initial capture or a backfill: it is skipped if it is added later, and later changes to it are not captured. Each of these warnings is logged once per file while the capture runs, for up to 1,000 files; after that, one more warning is logged and further files are not warned about. Files modified within the last 30 seconds are deferred to a later capture to allow for clock differences and files appearing in directory listings. Keep the server's clock synchronized.

Deleting a file or removing rows from a file does not remove previously captured documents from Estuary.

The advanced `ascendingKeys` option instead resumes after the last captured file path in lexicographic order. Enable this only when new files always have greater paths than previously captured files and existing files are never updated.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file. See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| -------- | ----- | ----------- | ---- | ---------------- |
| **`/host`** | Host | Hostname or IP address of the SMB server, without a port or share path. | string | Required |
| `/port` | Port | TCP port of the SMB server. | integer | `445` |
| **`/share`** | Share | Share name, for example `data`. | string | Required |
| **`/username`** | Username | Username for NTLM authentication. | string | Required |
| **`/password`** | Password | Password for authentication. This field is a secret. | string | Required |
| `/domain` | Domain | NTLM domain for authentication. | string | |
| **`/directory`** | Directory | Directory inside the share, including all subdirectories. Use `/` for the share root. | string | Required |
| `/matchFiles` | Match Files Regex | Only capture files whose path relative to the share root, starting with `/` (for example `/inbox/sub/a.csv`), matches this regular expression. For example, `.*\.csv$` captures CSV files. | string | |
| `/networkTunnel` | Network Tunnel | Connect through an SSH bastion host. Omit for a direct connection. | object | |
| `/networkTunnel/sshForwarding` | SSH Forwarding | Forward SMB connections through an SSH server. | object | |
| `/networkTunnel/sshForwarding/sshEndpoint` | SSH Endpoint | SSH server in the form `ssh://user@hostname[:port]`. The SSH port defaults to 22. | string | Required for SSH forwarding |
| `/networkTunnel/sshForwarding/privateKey` | SSH Private Key | Unencrypted SSH private key for the bastion user. This field is a multiline secret. | string | Required for SSH forwarding |
| `/advanced` | Advanced | Options for advanced users. | object | |
| `/advanced/ascendingKeys` | Ascending Keys | Resume listing after the last captured path. Requires files to be added in increasing lexicographic order. | boolean | `false` |
| `/parser` | Parser Configuration | Configures how files are parsed. | object | |
| `/parser/compression` | Compression | Compression method. Automatically detected when unset. | null, string | `null` |
| `/parser/format` | Format | File format. Automatically detected when unset. | object | `{"type":"auto"}` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| -------- | ----- | ----------- | ---- | ---------------- |
| **`/stream`** | Prefix | Path to the captured directory inside the share, such as `/inbox`. | string | Required |

### Sample

```yaml
captures:
  ${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-smb:v1
        config:
          host: files.example.com
          port: 445
          share: data
          username: <USERNAME>
          password: <PASSWORD>
          domain: EXAMPLE
          directory: /inbox
          matchFiles: '.*\.csv$'
          parser:
            format:
              type: csv
              config:
                delimiter: ","
                encoding: UTF-8
    bindings:
      - resource:
          stream: /inbox
        target: ${COLLECTION_NAME}
```

### Parsing files

The connector uses the same parser configuration as [SFTP](./sftp.md#advanced-parsing-sftp-files). Supported formats include CSV, JSON, Avro, Protobuf, and W3C Extended Log, with automatic format detection. ZIP, GZIP, and ZSTD compression are supported.

For most captures, leave `parser` unset to detect the format and compression automatically. Set `parser.format` or `parser.compression` when automatic detection is unsuitable. CSV options include the delimiter, encoding, headers, quoting, and line endings. Use `matchFiles` to select files compatible with your parser configuration.
