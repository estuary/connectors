#!/usr/bin/env bash
# Sample a locally-run connector's heap profile and RSS while a benchmark runs.
#
#   ./sample_heap.sh OUT_DIR [INTERVAL_SECONDS] [PROCESS_PATTERN]
#
# Every interval it saves the pprof heap profile from the connector's pprof
# server (localhost:6060) as OUT_DIR/heap-<unix ts>.pb.gz, the runtime
# MemStats lines as OUT_DIR/memstats-<ts>.txt, and appends the connector's
# RSS to OUT_DIR/rss.log. Summarize the samples with heap_breakdown.py.
set -u
OUT_DIR="${1:?usage: sample_heap.sh OUT_DIR [INTERVAL] [PATTERN]}"
INTERVAL="${2:-5}"
PATTERN="${3:-materialize-snowflake/connector}"
mkdir -p "$OUT_DIR"

while true; do
  ts=$(date +%s)
  # A --wrap launcher leaves several processes matching the pattern; the
  # connector itself is the one with the largest RSS.
  rss=$(pgrep -f "$PATTERN" | xargs -r ps -o rss= -p 2>/dev/null | sort -n | tail -1 | tr -d ' ')
  echo "$ts rss_kb=${rss:-0}" >> "$OUT_DIR/rss.log"
  if curl -sf --max-time 60 -o "$OUT_DIR/heap-$ts.pb.gz" http://localhost:6060/debug/pprof/heap; then
    curl -sf --max-time 60 "http://localhost:6060/debug/pprof/heap?debug=1" \
      | grep -E '^# (Sys|HeapSys|HeapInuse|HeapAlloc|HeapIdle|HeapReleased|StackInuse|NextGC|NumGC) ' \
      > "$OUT_DIR/memstats-$ts.txt"
  else
    rm -f "$OUT_DIR/heap-$ts.pb.gz"
  fi
  sleep "$INTERVAL"
done
