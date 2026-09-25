# source-sftp

## 2026-09-21

### Added

- `SSH Known Hosts` (`knownHosts`) configuration field. When set, the connector verifies the SFTP server's host key against the listed keys and refuses to connect if it does not match, protecting the capture against man-in-the-middle attacks. Takes OpenSSH `known_hosts` lines (the output of `ssh-keyscan`), one per line.

## v1, 2023-05-11

- Beginning of changelog.
