package smb

import (
	"fmt"
	"path"
	"strings"
)

// NormalizePath returns an absolute, share-relative path with forward slashes.
// Both slash styles are accepted, and an empty path names the share root.
func NormalizePath(p string) (string, error) {
	if strings.ContainsRune(p, '\x00') {
		return "", fmt.Errorf("invalid path %q: must contain no NUL bytes", p)
	}
	return path.Clean("/" + strings.ReplaceAll(p, `\`, "/")), nil
}

// ValidatePath checks an already normalized, absolute path within a share.
func ValidatePath(p string) error {
	if p != path.Clean(p) {
		return fmt.Errorf("invalid path '%s': must be a clean path (calculated clean path is '%s')", p, path.Clean(p))
	}
	if !strings.HasPrefix(p, "/") {
		return fmt.Errorf("invalid path '%s': path must be absolute (must start with a '/')", p)
	}
	if strings.ContainsAny(p, "\\\x00") {
		return fmt.Errorf("invalid path %q: must use '/' separators and contain no NUL bytes", p)
	}
	return nil
}

// Path translates a normalized, absolute path to SMB's relative path syntax.
// The empty SMB path names the share root.
func Path(p string) string {
	return strings.ReplaceAll(strings.TrimPrefix(p, "/"), "/", `\`)
}
