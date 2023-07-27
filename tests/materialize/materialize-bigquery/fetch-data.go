package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var deleteTable = flag.Bool("delete", false, "delete the table instead of dumping its contents")
var deleteSpecs = flag.Bool("delete-specs", false, "stored materialize checkpoint and specs")

func main() {
	flag.Parse()

	tables := flag.Args()
	if len(tables) != 1 {
		log.Fatal("must provide table name as an argument")
	}

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

	ctx := context.Background()

	client, err := bigquery.NewClient(
		context.Background(),
		projectID,
		option.WithCredentialsJSON([]byte(saKey)))
	if err != nil {
		log.Fatal(fmt.Errorf("building bigquery client: %w", err))
	}

	// Handle cleanup cases of for dropping a table and deleting the stored materialization spec &
	// checkpoint if flags were provided.
	if *deleteTable {
		if err := client.DatasetInProject(projectID, dataset).Table(tables[0]).Delete(ctx); err != nil {
			fmt.Println(fmt.Errorf("could not drop table %s: %w", tables[0], err))
		}
		os.Exit(0)
	} else if *deleteSpecs {
		query := fmt.Sprintf(
			"delete from %s.flow_checkpoints_v1 where materialization='tests/materialize-bigquery/materialize';delete from %s.flow_materializations_v2 where materialization='tests/materialize-bigquery/materialize';",
			dataset,
			dataset,
		)

		job, err := client.Query(query).Run(ctx)
		if err != nil {
			fmt.Println(fmt.Errorf("could not delete stored materialization spec/checkpoint: %w", err))
			os.Exit(1)
		}

		if _, err := job.Wait(ctx); err != nil {
			fmt.Println(fmt.Errorf("could not delete stored materialization spec/checkpoint: %w", err))
			os.Exit(1)
		}

		os.Exit(0)
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

	// Sort by ID
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(int64) < rows[j][0].(int64)
	})

	var enc = json.NewEncoder(os.Stdout)
	for _, row := range rows {
		enc.Encode(row)
	}
}
