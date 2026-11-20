# Changelog

## 2026-11-11

### Added

- Capture files from SMB2 and SMB3 shares, including subdirectories, with incremental capture of new and modified files.
- Require SMB message signing and reject guest and anonymous sessions. Encrypted SMB3 frames with a nonempty payload are accepted only after an authenticated session has negotiated encryption and is enabled.
- Fail the capture on an unreadable subdirectory so its files are captured once access is restored. Unreadable system folders such as `System Volume Information`, `DfsrPrivate` and `lost+found` are skipped.
- Read files when the configured directory, or one of its parents, is a Windows directory symbolic link at the share root with a relative target.
- Capture file links, deduplicated files and other reparse points beneath such a directory link by resolving the link's relative target, rather than skipping them.
- Support optional SSH tunneling through a bastion host with `networkTunnel.sshForwarding`, using the shared network tunnel implementation.
- Use a file's change time, or failing that its creation time, when the server reports no valid modification time. A file captured by its creation time is not captured again when it changes, and a file with none of these valid is captured only by the initial capture or a backfill; both log a warning once per file, for up to 1,000 files.
