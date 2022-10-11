package main

import (
	"fmt"
	"strings"

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
	// the prefix up to the first "/documents" is unnecessary, so we remove that
	var parts = strings.SplitN(collection, "/documents/", 2)
	if len(parts) < 2 {
		panic(fmt.Sprintf("collection path does not match expectations: %s", collection))
	}
	var after = parts[1]

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

// Transforms a document path to a "resource path" which abstracts over document
// IDs and omits the database path prefix. For example:
//
//	"projects/blissful-jet-364120/databases/(default)/documents/users/OLgLVvZnykvFR4ZyqUuS/messages/136434"
//	=> "users/*/messages"
func documentToResourcePath(documentPath string) resourcePath {
	return collectionToResourcePath(documentPath)
}

func retryableStatus(err error) bool {
	var code = status.Code(err)
	return code == codes.Unknown ||
		code == codes.DeadlineExceeded ||
		code == codes.ResourceExhausted ||
		code == codes.Internal ||
		code == codes.Unavailable
}
