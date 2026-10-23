#!/bin/sh
# SessionStart: load the permission gates into Claude's shell. The sops wrapper
# goes first on PATH; the Bruno gate loads into every Node process and does
# nothing outside `bru`. Each Bash command runs the env file first. Each line
# adds its entry only when it is missing, because a resume or compaction runs
# this hook again.
[ -n "$CLAUDE_ENV_FILE" ] || exit 0
cat >>"$CLAUDE_ENV_FILE" <<EOF
case ":\$PATH:" in *":$CLAUDE_PROJECT_DIR/.claude/bin:"*) ;; *) export PATH="$CLAUDE_PROJECT_DIR/.claude/bin:\$PATH" ;; esac
case "\${NODE_OPTIONS:-}" in *bru-gate.js*) ;; *) export NODE_OPTIONS="--require \"$CLAUDE_PROJECT_DIR/.claude/hooks/bru-gate.js\" \${NODE_OPTIONS:-}" ;; esac
EOF
exit 0
