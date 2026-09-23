package main

import (
	"bytes"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const sampleSpec = `captures:
  acmeCo/source-postgres:
    expectPubId: 00aa11bb22cc33dd
    endpoint:
      connector:
        image: ghcr.io/estuary/source-postgres:v1
        config: acmeCo.config.yaml
    bindings:
      - resource:
          table: foo
        target: acmeCo/foo
    shards:
      maxTxnDuration: 5m
`

func rewrite(t *testing.T, in string, disabled bool) string {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(in), &doc); err != nil {
		t.Fatal(err)
	}
	task, err := findTaskNode(&doc, "acmeCo/source-postgres")
	if err != nil {
		t.Fatal(err)
	}
	if disabled {
		setBool(mapChild(task, "shards", true), "disable", true)
	} else if shards := mapChild(task, "shards", false); shards != nil {
		deleteKey(shards, "disable")
		if len(shards.Content) == 0 {
			deleteKey(task, "shards")
		}
	}
	var buf bytes.Buffer
	var enc = yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func TestDisableEnableRoundTrip(t *testing.T) {
	disabled := rewrite(t, sampleSpec, true)
	if !strings.Contains(disabled, "disable: true") || !strings.Contains(disabled, "expectPubId: 00aa11bb22cc33dd") {
		t.Fatalf("unexpected disabled spec:\n%s", disabled)
	}
	if !strings.Contains(disabled, "maxTxnDuration: 5m") {
		t.Fatalf("existing shards settings lost:\n%s", disabled)
	}

	enabled := rewrite(t, disabled, false)
	if strings.Contains(enabled, "disable") || !strings.Contains(enabled, "maxTxnDuration: 5m") {
		t.Fatalf("unexpected enabled spec:\n%s", enabled)
	}
}

func TestEnableDropsEmptyShards(t *testing.T) {
	var spec = strings.Replace(sampleSpec, "      maxTxnDuration: 5m\n", "      disable: true\n", 1)
	enabled := rewrite(t, spec, false)
	if strings.Contains(enabled, "shards") {
		t.Fatalf("empty shards block should be removed:\n%s", enabled)
	}
}
