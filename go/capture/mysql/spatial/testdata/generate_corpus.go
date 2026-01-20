//go:build ignore

// This program generates the test corpus for the spatial package by:
// 1. Reading WKT examples from testdata/wkt_examples.txt
// 2. Inserting each into MySQL via ST_GeomFromText()
// 3. Reading back HEX(geom) and ST_AsText(geom)
// 4. Writing valid test cases to testdata/wkt_corpus.json
//
// Run with: go run generate_corpus_test.go
package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// TestCase represents a single test case in the corpus
type TestCase struct {
	InputWKT   string `json:"input_wkt"`
	SRID       int    `json:"srid"`
	InternalHex string `json:"internal_hex"`
	OutputWKT  string `json:"output_wkt"`
}

func main() {
	// Connect to MySQL
	db, err := sql.Open("mysql", "root:secret1234@tcp(localhost:3306)/test")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to MySQL: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Verify connection
	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to ping MySQL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Connected to MySQL")

	// Read WKT examples
	inputFile, err := os.Open("testdata/wkt_examples.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open input file: %v\n", err)
		os.Exit(1)
	}
	defer inputFile.Close()

	var testCases []TestCase
	var validCount, invalidCount int

	scanner := bufio.NewScanner(inputFile)
	// Increase buffer size for long lines
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	sridsToTest := []int{0, 4326} // Test with SRID 0 and 4326

	for scanner.Scan() {
		inputWKT := strings.TrimSpace(scanner.Text())
		if inputWKT == "" {
			continue
		}

		// Test with different SRIDs
		for _, srid := range sridsToTest {
			testCase, err := generateTestCase(db, inputWKT, srid)
			if err != nil {
				// Invalid WKT - skip silently for SRID != 0, log once for SRID 0
				if srid == 0 {
					invalidCount++
				}
				continue
			}
			testCases = append(testCases, *testCase)
			if srid == 0 {
				validCount++
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Processed %d valid, %d invalid WKT examples\n", validCount, invalidCount)
	fmt.Printf("Generated %d test cases (with multiple SRIDs)\n", len(testCases))

	// Write output JSON
	outputFile, err := os.Create("testdata/wkt_corpus.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer outputFile.Close()

	encoder := json.NewEncoder(outputFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(testCases); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write JSON: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Wrote test corpus to testdata/wkt_corpus.json\n")
}

func generateTestCase(db *sql.DB, inputWKT string, srid int) (*TestCase, error) {
	// Use a query that inserts and reads back in one go
	// This avoids needing to create a table
	var internalHex, outputWKT string

	query := `SELECT HEX(ST_GeomFromText(?, ?)), ST_AsText(ST_GeomFromText(?, ?))`
	err := db.QueryRow(query, inputWKT, srid, inputWKT, srid).Scan(&internalHex, &outputWKT)
	if err != nil {
		return nil, err
	}

	return &TestCase{
		InputWKT:    inputWKT,
		SRID:        srid,
		InternalHex: internalHex,
		OutputWKT:   outputWKT,
	}, nil
}
