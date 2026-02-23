package main

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type collectionGroupID = string
type resourcePath = string

// Our resource paths are formatted with parent_collection/*/nested_collection
// in order to query documents for such collections we use Firestore's Collection Groups
// which operate on the name of the last collection. Note that this name might not be unique
// so another check is necessary to make sure we do not mix documents from nested collections
// that have the same ID, but different parent collections
func getLastCollectionGroupID(path resourcePath) collectionGroupID {
	var pieces = strings.Split(path, "/")
	return pieces[len(pieces)-1]
}

// Transforms a collection path to a "resource path" which abstracts over
// document IDs and omits the database path prefix. For example:
//
//	"projects/blissful-jet-364120/databases/(default)/documents/users/OLgLVvZnykvFR4ZyqUuS/messages"
//	=> "users/*/messages"
func collectionToResourcePath(collection string) resourcePath {
	var after = trimDatabasePath(collection)

	// we now need to get rid of document references, which appear after every collection name
	var pieces = strings.Split(after, "/")
	var cleanedPath = ""

	for i, piece := range pieces {
		if i%2 == 0 {
			cleanedPath = cleanedPath + "/*/" + piece
		}
	}

	return strings.Trim(cleanedPath, "/*/")
}

// Transforms a document or collection path by stripping off the database path
// prefix. For example:
//
//	"projects/blissful-jet-364120/databases/(default)/documents/users/OLgLVvZnykvFR4ZyqUuS/messages/123"
//	=> "users/OLgLVvZnykvFR4ZyqUuS/messages/123"
func trimDatabasePath(path string) string {
	var parts = strings.SplitN(path, "/documents/", 2)
	if len(parts) != 2 {
		panic(fmt.Sprintf("database path %q does not contain '/documents/' separator", path))
	}
	return parts[1]
}

// Transforms a document path to a "resource path" which abstracts over document
// IDs and omits the database path prefix. For example:
//
//	"projects/blissful-jet-364120/databases/(default)/documents/users/OLgLVvZnykvFR4ZyqUuS/messages/136434"
//	=> "users/*/messages"
func documentToResourcePath(documentPath string) resourcePath {
	return collectionToResourcePath(documentPath)
}

// parseDatabasePath extracts projectID and databaseID from a database path.
// Input format: "projects/{projectID}/databases/{databaseID}"
func parseDatabasePath(path string) (projectID, databaseID string, err error) {
	var parts = strings.Split(path, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "databases" {
		return "", "", fmt.Errorf("invalid database path %q: expected projects/{projectID}/databases/{databaseID}", path)
	}
	return parts[1], parts[3], nil
}

// resolveDatabasePath returns the database path from the config, or auto-detects
// it from the credentials if not specified. Returns the resolved path along with
// the extracted projectID and databaseID.
func resolveDatabasePath(ctx context.Context, configPath string, credsOpt option.ClientOption) (databasePath, projectID, databaseID string, err error) {
	databasePath = configPath
	if databasePath == "" {
		creds, credsErr := transport.Creds(ctx, credsOpt)
		if credsErr != nil {
			return "", "", "", fmt.Errorf("unable to get credentials: %w", credsErr)
		}
		if creds == nil || creds.ProjectID == "" {
			return "", "", "", fmt.Errorf("unable to determine project ID from credentials (set 'database' config property)")
		}
		databasePath = fmt.Sprintf("projects/%s/databases/(default)", creds.ProjectID)
	}

	projectID, databaseID, err = parseDatabasePath(databasePath)
	if err != nil {
		return "", "", "", err
	}
	return databasePath, projectID, databaseID, nil
}

func retryableStatus(err error) bool {
	var code = status.Code(err)
	return code == codes.Unknown ||
		code == codes.DeadlineExceeded ||
		code == codes.Internal ||
		code == codes.Unavailable
}
