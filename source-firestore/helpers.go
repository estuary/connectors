package main

import (
	"fmt"
	"strings"
)

// Nested collections are referred to by their parent document in Firestore
// e.g. parent_collection/my_doc_id123/nested_collection
// however in resource spec, we use wildcard /*/ to refer to all nested_collections under parent_collection
// this function matches collection paths with resource paths
func collectionMatchesResourcePath(collectionPath string, resourcePath string) bool {
	return collectionToResourcePath(collectionPath) == resourcePath
}

// Searches in list of bindings for one that matches the collection path
func findBindingForCollectionPath(collectionPath string, bindings []resource) int {
	for index, binding := range bindings {
		if collectionMatchesResourcePath(collectionPath, binding.Path) {
			return index
		}
	}

	return -1
}

// Our resource paths are formatted with parent_collection/*/nested_collection
// in order to query documents for such collections we use Firestore's Collection Groups
// which operate on the name of the last collection. Note that this name might not be unique
// so another check is necessary to make sure we do not mix documents from nested collections
// that have the same ID, but different parent collections
func getLastCollectionGroupID(resourcePath string) string {
	var pieces = strings.Split(resourcePath, "/")
	return pieces[len(pieces)-1]
}

func isRootCollection(resourcePath string) bool {
	return !strings.Contains(resourcePath, "/")
}

// Firestore's original collection path is very verbose, and it includes the parent document ID
// This function creates a path for a collection which only refers to its parent collections, not documents
// e.g. projects/hello-flow-mahdi/databases/(default)/documents/group/OLgLVvZnykvFR4ZyqUuS/group
// becomes group/*/group
func collectionToResourcePath(collection string) string {
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

func documentToResourcePath(documentPath string) string {
	if idx := strings.LastIndex(documentPath, "/"); idx >= 0 {
		return collectionToResourcePath(documentPath[:idx])
	}
	return documentPath
}
