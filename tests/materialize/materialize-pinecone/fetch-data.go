package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/pinecone-io/go-pinecone/pinecone"
)

var printVectors = flag.Bool("print-vectors", false, "Print the full vectors rather than a placeholder. May be useful for debugging.")
var clearNamespace = flag.Bool("clear-namespace", false, "remove all vectors from the namespace")

func main() {
	ctx := context.Background()

	flag.Parse()
	args := flag.Args()

	if len(args) != 1 {
		log.Fatal("must provide namespace to query or clear as a single argument")
	}

	pc, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey:    os.Getenv("PINECONE_API_KEY"),
		SourceTag: "estuary",
	})
	if err != nil {
		log.Fatal(err)
	}

	idx, err := pc.DescribeIndex(ctx, os.Getenv("PINECONE_INDEX"))
	if err != nil {
		log.Fatal(err)
	}

	conn, err := pc.Index(pinecone.NewIndexConnParams{Host: idx.Host, Namespace: args[0]})
	if err != nil {
		log.Fatal(err)
	}

	if *clearNamespace {
		if err := conn.DeleteAllVectorsInNamespace(ctx); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("cleared namespace %s\n", args[0])
		os.Exit(0)
	}

	var ids []string

	// Vectors take a long time to be queryable in pinecone serverless
	// instances.
	for attempt := 0; attempt < 6; attempt++ {
		idRes, err := conn.ListVectors(ctx, &pinecone.ListVectorsRequest{})
		if err != nil {
			log.Fatal(err)
		}

		for _, id := range idRes.VectorIds {
			ids = append(ids, *id)
		}

		if len(ids) > 0 {
			break
		}
		time.Sleep(10 * time.Second)
	}

	vecRes, err := conn.FetchVectors(ctx, ids)
	if err != nil {
		log.Fatal(err)
	}

	var vecs []pinecone.Vector

	for _, v := range vecRes.Vectors {
		vecs = append(vecs, *v)
	}

	// Deterministic output ordering, sorted by ID.
	sort.Slice(vecs, func(i, j int) bool {
		return vecs[i].Id < vecs[j].Id
	})

	// The vectors themselves are very long and the floating point values of any point in the vector
	// may vary between invocations, so there's no real way to snapshot the output. The flag
	// print-vectors can be used to output the vectors instead of masking their values for local
	// testing. You can get a decent qualitative idea of if a vector is wrong by how many of its
	// points are different and how different they are.
	if !*printVectors {
		for idx := range vecs {
			vecs[idx].Values = nil
		}
	}

	out := map[string]any{
		"namespace": args[0],
		"results":   vecs,
	}

	formatted, err := json.MarshalIndent(out, "", "\t")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(formatted))
}
