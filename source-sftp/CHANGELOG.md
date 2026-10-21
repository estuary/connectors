# source-sftp

## 2026-10-01

### Changed

- `SSH Known Hosts` (`knownHosts`) is now required unless `Skip Host Key Verification` (`skipHostKeyVerification`) is set. A configuration with neither is rejected.

### Added

- `Skip Host Key Verification` (`skipHostKeyVerification`) is now shown in the UI and documented, as the explicit way to connect without verifying the server's host key.

## 2026-09-21

### Added

- `SSH Known Hosts` (`knownHosts`) configuration field. When set, the connector verifies the SFTP server's host key against the listed keys and refuses to connect if it does not match, protecting the capture against man-in-the-middle attacks. Takes OpenSSH `known_hosts` lines (the output of `ssh-keyscan`), one per line.

## v1, 2023-05-11

- Beginning of changelog.
