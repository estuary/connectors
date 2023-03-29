package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func main() {
	saKey, ok := os.LookupEnv("GCP_SERVICE_ACCOUNT_KEY")
	if !ok {
		log.Fatal("missing GCP_SERVICE_ACCOUNT_KEY environment variable")
	}

	projectID, ok := os.LookupEnv("GCP_BQ_PROJECT_ID")
	if !ok {
		log.Fatal("missing GCP_BQ_PROJECT_ID environment variable")
	}

	dataset, ok := os.LookupEnv("GCP_BQ_DATASET")
	if !ok {
		log.Fatal("missing GCP_BQ_DATASET environment variable")
	}

	region, ok := os.LookupEnv("GCP_BQ_REGION")
	if !ok {
		log.Fatal("missing GCP_BQ_REGION environment variable")
	}

	tables := os.Args[1:]
	if len(tables) != 1 {
		log.Fatal("must provide table name as an argument")
	}

	ctx := context.Background()

	client, err := bigquery.NewClient(
		context.Background(),
		projectID,
		option.WithCredentialsJSON([]byte(saKey)))
	if err != nil {
		log.Fatal(fmt.Errorf("building bigquery client: %w", err))
	}

	queryString := fmt.Sprintf("SELECT * FROM %s", strings.Join([]string{projectID, dataset, tables[0]}, "."))

	query := client.Query(queryString)
	query.Location = region

	job, err := query.Run(ctx)
	if err != nil {
		log.Fatal(fmt.Errorf("bigquery run: %w", err))
	}

	it, err := job.Read(ctx)
	if err != nil {
		log.Fatal(fmt.Errorf("bigquery job read: %w", err))
	}

	rows := [][]bigquery.Value{}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(fmt.Errorf("bigquery job iterator next: %w", err))
		}

		rows = append(rows, values)
	}

	// Remove the "flow document" entry, which contains a random uuid.
	cleaned := [][]bigquery.Value{}
	for _, r := range rows {
		vals := []bigquery.Value{}

		for _, v := range r {
			if val, ok := v.(string); ok {
				if strings.Contains(val, "uuid") {
					continue
				}
			}
			vals = append(vals, v)
		}

		cleaned = append(cleaned, vals)
	}

	// Sort by ID
	sort.Slice(cleaned, func(i, j int) bool {
		return cleaned[i][0].(int64) < cleaned[j][0].(int64)
	})

	var enc = json.NewEncoder(os.Stdout)
	for _, row := range cleaned {
		enc.Encode(row)
	}
}
