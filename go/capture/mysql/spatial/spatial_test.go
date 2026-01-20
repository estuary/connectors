package spatial

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

type testCase struct {
	InputWKT    string `json:"input_wkt"`
	SRID        int    `json:"srid"`
	InternalHex string `json:"internal_hex"`
	OutputWKT   string `json:"output_wkt"`
}

func TestParseToWKT(t *testing.T) {
	// Load test corpus
	corpusData, err := os.ReadFile("testdata/wkt_corpus.json")
	if err != nil {
		t.Fatalf("failed to load test corpus: %v", err)
	}

	var corpus []testCase
	if err := json.Unmarshal(corpusData, &corpus); err != nil {
		t.Fatalf("failed to parse test corpus: %v", err)
	}

	if len(corpus) == 0 {
		t.Fatal("test corpus is empty")
	}

	// Run all test cases, collecting failures
	var failures []testCase
	for _, tc := range corpus {
		// Decode hex to bytes
		data, err := hex.DecodeString(tc.InternalHex)
		if err != nil {
			t.Errorf("failed to decode hex for %q (SRID %d): %v", tc.InputWKT, tc.SRID, err)
			continue
		}

		// Parse to WKT
		got, err := ParseToWKT(data)
		if err != nil {
			failures = append(failures, tc)
			continue
		}

		// Compare against expected output
		if got != tc.OutputWKT {
			failures = append(failures, tc)
		}
	}

	// Report results
	if len(failures) > 0 {
		t.Errorf("%d/%d test cases failing", len(failures), len(corpus))

		// Count failures by SRID
		sridCounts := make(map[int]int)
		for _, tc := range failures {
			sridCounts[tc.SRID]++
		}
		t.Errorf("Failures by SRID: %v", sridCounts)

		// Show first 5 failing cases
		t.Errorf("First failing cases:")
		count := 0
		for _, tc := range failures {
			if count >= 5 {
				break
			}
			count++
			data, _ := hex.DecodeString(tc.InternalHex)
			got, err := ParseToWKT(data)
			if err != nil {
				t.Errorf("  %d (SRID %d): error: %v", count, tc.SRID, err)
				t.Errorf("      expected: %q", tc.OutputWKT)
			} else {
				t.Errorf("  %d (SRID %d): got:      %q", count, tc.SRID, got)
				t.Errorf("      expected: %q", tc.OutputWKT)
			}
		}
	}
}
