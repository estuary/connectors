package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
)

type fetchReq struct {
	TopK            int       `json:"topK"`
	IncludeMetadata bool      `json:"includeMetadata"`
	IncludeValues   bool      `json:"includeValues"`
	Namespace       string    `json:"namespace"`
	Vector          []float64 `json:"vector"`
}

type fetchRes struct {
	Matches   []match       `json:"matches"`
	Namespace string        `json:"namespace"`
	Results   []interface{} `json:"results"`
}

type match struct {
	Id       string                 `json:"id"`
	Metadata map[string]interface{} `json:"metadata"`
	Score    int                    `json:"score"`
	Values   interface{}            `json:"values"` // Slice of float64
}

var printVectors = flag.Bool("print-vectors", false, "Print the full vectors rather than a placeholder. May be useful for debugging.")

func main() {
	index, ok := os.LookupEnv("PINECONE_INDEX")
	if !ok {
		log.Fatal("must set PINECONE_INDEX")
	}

	env, ok := os.LookupEnv("PINECONE_ENVIRONMENT")
	if !ok {
		log.Fatal("must set PINECONE_ENVIRONMENT")
	}

	apiKey, ok := os.LookupEnv("PINECONE_API_KEY")
	if !ok {
		log.Fatal("must set PINECONE_API_KEY")
	}

	projectId, ok := os.LookupEnv("PINECONE_PROJECT_ID")
	if !ok {
		log.Fatal("must set PINECONE_PROJECT_ID")
	}

	flag.Parse()
	args := flag.Args()

	if len(args) != 1 {
		log.Fatal("must provide namespace to query as a single argument")
	}

	ctx := context.Background()

	fetch := fetchReq{
		TopK:            10,
		IncludeMetadata: true,
		IncludeValues:   true,
		Namespace:       args[0],
		Vector:          []float64{},
	}

	// Dummy query vector containing 1536 (the required length for text-embedding-ada-002) 0's.
	for i := 0; i < 1536; i++ {
		fetch.Vector = append(fetch.Vector, 0)
	}

	body := new(bytes.Buffer)
	if err := json.NewEncoder(body).Encode(&fetch); err != nil {
		log.Fatal(err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("https://%s-%s.svc.%s.pinecone.io/query", index, projectId, env), body)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Api-Key", apiKey)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	out := fetchRes{}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		log.Fatal(err)
	}

	// Deterministic output ordering, sorted by ID.
	sort.Slice(out.Matches, func(i, j int) bool {
		return out.Matches[i].Id < out.Matches[j].Id
	})

	// The vectors themselves are very long and the floating point values of any point in the vector
	// may vary between invocations, so there's no real way to snapshot the output. The flag
	// print-vectors can be used to output the vectors instead of masking their values for local
	// testing. You can get a decent qualitative idea of if a vector is wrong by how many of its
	// points are different and how different they are.
	if !*printVectors {
		for idx := range out.Matches {
			out.Matches[idx].Values = "<VECTOR>"
		}
	}

	formatted, err := json.MarshalIndent(out, "", "\t")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(formatted))
}
