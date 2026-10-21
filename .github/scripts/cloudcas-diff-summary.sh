#!/usr/bin/env bash
# Prints a Markdown summary of how the CA bundles in go/mysql/tls/cloudcas/
# differ from HEAD, one line per certificate added or removed. Used by the
# mysql-cloud-cas workflow, since a PEM diff is unreadable.
set -euo pipefail
DIR=go/mysql/tls/cloudcas

# Reads a PEM bundle on stdin and prints one sorted line per certificate: its
# subject and SHA-256 fingerprint. The fingerprint matters because a renewed CA
# often keeps its old subject.
certs() {
	local tmp
	tmp=$(mktemp -d)
	# openssl x509 reads only the first certificate of its input, so split the
	# bundle into one file per certificate first.
	awk -v dir="$tmp" '/BEGIN CERTIFICATE/{n++} n{print > (dir "/" n ".pem")}'
	for f in "$tmp"/*.pem; do
		[ -e "$f" ] || continue
		openssl x509 -in "$f" -noout -subject -fingerprint -sha256 -nameopt RFC2253 |
			paste -sd ' ' -
	done | sort
	rm -rf "$tmp"
}

# Covers bundles that were added, changed, or deleted since HEAD.
files=$({ git ls-tree --name-only HEAD "$DIR/"; ls "$DIR"/*.pem; } | grep '\.pem$' | sort -u)
for file in $files; do
	before=$(git show "HEAD:$file" 2>/dev/null | certs || true)
	after=$([ -e "$file" ] && certs <"$file" || true)
	[ "$before" = "$after" ] && continue
	echo "#### \`$(basename "$file")\`"
	echo
	echo '```diff'
	# An added or deleted bundle has an empty side, which isn't a certificate.
	diff <(echo "$before") <(echo "$after") | grep -E '^[<>] .' |
		sed -e 's/^</-/' -e 's/^>/+/' || true
	echo '```'
	echo
done
