package main

import (
	"context"
	"fmt"
	"os"

	firestore "cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	flags "github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type AddTimestampFieldCmd struct {
	Collections []string `short:"c",long:"collection"`
	FieldName   string   `long:"field-name",default:"updated_at"`
	Credentials string   `long:"credentials"`
}

func (c *AddTimestampFieldCmd) Execute(_ []string) error {
	return AddTimestampField(*c)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func AddTimestampField(args AddTimestampFieldCmd) error {
	fmt.Println("This script is useful for initially adding a monotonically")
	fmt.Println("increasing, unique timestamp to all of your documents in some collections.")
	fmt.Println("However, note that it is necessary for you to update this field")
	fmt.Println("as your documents get updated in order for Flow to be able to")
	fmt.Println("become aware of your changes.")
	fmt.Println("The field is filled using `ServerTimestamp`, and so all updates")
	fmt.Println("to this field must also be done using the same method.")
	fmt.Println("")

	ctx := context.Background()
	sa := option.WithCredentialsFile(args.Credentials)
	log.Info(args.Credentials)
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		return err
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	var fieldName = args.FieldName
	if fieldName == "" {
		fieldName = "updated_at"
	}

	var collections = client.Collections(ctx)
	for {
		var collection, err = collections.Next()

		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		var collectionLog = log.WithFields(log.Fields{
			"collection": collection.ID,
		})
		if !contains(args.Collections, collection.ID) {
			collectionLog.Info("Skipping collection")
			continue
		}
		collectionLog.Info("Updating collection")

		var query = collection.Query
		var docs = query.Documents(ctx)

		for {
			var doc, err = docs.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				return err
			}
			_, err = doc.Ref.Update(ctx, []firestore.Update{
				{Path: fieldName, Value: firestore.ServerTimestamp},
			})

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func main() {
	// Try to handle add-timestamp-field
	var parser = flags.NewParser(nil, flags.Default)
	var addTimestampField = AddTimestampFieldCmd{}
	parser.AddCommand("add-timestamp-field", "add a timestamp field to your collections", "Will add a new field to listed collections that represent the server timestamp of their last update.", &addTimestampField)

	// This will actually execute the given subcommand because that's clearly what "parse" means /s.
	var _, err = parser.Parse()
	if err != nil {
		// We don't log the error because flags.Default includes flags.PrintErrors,
		// and thus Parse() has already done so on our behalf.
		os.Exit(1)
	}
}
