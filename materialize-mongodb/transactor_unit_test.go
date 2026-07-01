package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// storeAndReload runs storeDocument and then simulates the MongoDB store + load
// round-trip (BSON marshal/unmarshal followed by the connector's load
// sanitization), returning both the stored BSON document and the JSON the
// runtime would parse to re-derive the key.
func storeAndReload(t *testing.T, raw json.RawMessage, idValue string, deltaUpdates bool, key tuple.Tuple, keyPtrs []string) (bson.M, []byte) {
	t.Helper()
	var restorations []keyRestoration
	for i, ptr := range keyPtrs {
		restorations = append(restorations, keyRestoration{tupleIndex: i, tokens: parsePointerTokens(ptr)})
	}
	doc, err := storeDocument(raw, idValue, deltaUpdates, key, restorations)
	require.NoError(t, err)

	encoded, err := bson.Marshal(doc)
	require.NoError(t, err)
	var reloaded bson.M
	require.NoError(t, bson.Unmarshal(encoded, &reloaded))
	loadedJSON, err := json.Marshal(sanitizedLoadedDocument(reloaded))
	require.NoError(t, err)

	return doc, loadedJSON
}

// numberAt decodes loadedJSON with UseNumber and returns the json.Number at the
// given top-level field, so tests can assert exact (non-lossy) round-tripping.
func numberAt(t *testing.T, loadedJSON []byte, field string) json.Number {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(loadedJSON))
	dec.UseNumber()
	var m map[string]interface{}
	require.NoError(t, dec.Decode(&m))
	n, ok := m[field].(json.Number)
	require.True(t, ok, "field %q is not a number: %v", field, m[field])
	return n
}

// TestStoreDocumentPreservesLargeIntegerKey is the regression test for the
// duplicate-key crash loop on large integer keys. A key beyond float64's exact
// integer range (2^53) must round-trip through a store and a subsequent load
// without losing precision: the runtime re-derives a store's key from the
// document body returned by Load (a Loaded response carries no key), so a lossy
// EVENT_ID makes the runtime fail to match the load, report Exists=false, and
// the connector then issues a strict InsertOne for an _id that already exists →
// E11000, permanently.
func TestStoreDocumentPreservesLargeIntegerKey(t *testing.T) {
	// A real colliding EVENT_ID from the field: ~-9.2e18, far beyond 2^53, and
	// odd (so not representable as a float64, whose ULP at this magnitude is 2048).
	const eventID int64 = -9223341701339290697
	raw := json.RawMessage(fmt.Sprintf(`{"EVENT_ID":%d,"EVENT_TYPE":"u","NEW_VALUE":1.5}`, eventID))
	// _id is the exact packed key; it is always faithful. The bug is in the body.
	const packedKeyHex = "0c80001b97099fe3b6"

	_, loadedJSON := storeAndReload(t, raw, packedKeyHex, false,
		tuple.Tuple{eventID}, []string{"/EVENT_ID"})

	// The runtime re-derives the key by parsing EVENT_ID out of the loaded doc.
	require.Equal(t, fmt.Sprint(eventID), numberAt(t, loadedJSON, "EVENT_ID").String(),
		"EVENT_ID must round-trip exactly; a lossy value re-derives a different key and breaks load matching")
}

// TestStoreDocumentLeavesNonKeyIntegersAsDouble verifies the key-scoped design:
// only key fields get exact integer precision, while non-key numbers decode as
// float64 exactly as before. This keeps BSON types stable across documents (a
// non-key field never flips between int64 and double based on its value) and
// avoids a fleet-wide type change on existing collections.
func TestStoreDocumentLeavesNonKeyIntegersAsDouble(t *testing.T) {
	const key int64 = 1
	const nonKey int64 = 9223372036854775807 // MaxInt64, far beyond 2^53
	raw := json.RawMessage(fmt.Sprintf(`{"id":%d,"COUNT":%d}`, key, nonKey))

	doc, loadedJSON := storeAndReload(t, raw, "00", false,
		tuple.Tuple{key}, []string{"/id"})

	require.IsType(t, int64(0), doc["id"], "key field must be stored as int64")
	require.IsType(t, float64(0), doc["COUNT"], "non-key integer must stay float64 (stable BSON type)")

	require.Equal(t, fmt.Sprint(key), numberAt(t, loadedJSON, "id").String(),
		"key round-trips exactly")
	require.NotEqual(t, fmt.Sprint(nonKey), numberAt(t, loadedJSON, "COUNT").String(),
		"non-key integer beyond 2^53 is expected to be lossy (unchanged pre-PR behavior)")
}

// TestStoreDocumentNestedKeyPointer exercises a key at a nested JSON pointer,
// guarding setAtPointer's traversal of both the root bson.M and nested
// map[string]interface{} produced by json.Unmarshal.
func TestStoreDocumentNestedKeyPointer(t *testing.T) {
	const id int64 = 9223372036854775807
	raw := json.RawMessage(fmt.Sprintf(`{"a":{"b":{"id":%d}}}`, id))

	_, loadedJSON := storeAndReload(t, raw, "00", false,
		tuple.Tuple{id}, []string{"/a/b/id"})

	var got struct {
		A struct {
			B struct {
				ID json.Number `json:"id"`
			} `json:"b"`
		} `json:"a"`
	}
	dec := json.NewDecoder(bytes.NewReader(loadedJSON))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&got))
	require.Equal(t, fmt.Sprint(id), got.A.B.ID.String(), "nested key must round-trip exactly")
}

// TestStoreDocumentArrayNestedKey exercises a key located beneath a JSON array
// element (a legal Flow key location). setAtPointer must traverse the
// []interface{} that json.Unmarshal produces and restore the exact int64;
// otherwise the lossy float64 re-derives a mismatched key and provokes the
// duplicate-key crash loop.
func TestStoreDocumentArrayNestedKey(t *testing.T) {
	const id int64 = 9223372036854775807 // MaxInt64, far beyond 2^53
	raw := json.RawMessage(fmt.Sprintf(`{"items":[{"id":%d}]}`, id))

	_, loadedJSON := storeAndReload(t, raw, "00", false,
		tuple.Tuple{id}, []string{"/items/0/id"})

	var got struct {
		Items []struct {
			ID json.Number `json:"id"`
		} `json:"items"`
	}
	dec := json.NewDecoder(bytes.NewReader(loadedJSON))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&got))
	require.Len(t, got.Items, 1)
	require.Equal(t, fmt.Sprint(id), got.Items[0].ID.String(),
		"array-nested key must round-trip exactly")
}

// TestStoreDocumentIdCollisionKey guards the ordering of key overwrite vs the
// _id-collision rename: a key located at /_id must be corrected in place before
// the rename moves it to _flow_id, so it round-trips exactly after the load path
// reverses the rename.
func TestStoreDocumentIdCollisionKey(t *testing.T) {
	const id int64 = 9223372036854775807
	raw := json.RawMessage(fmt.Sprintf(`{"_id":%d}`, id))

	_, loadedJSON := storeAndReload(t, raw, "00", false,
		tuple.Tuple{id}, []string{"/_id"})

	require.Equal(t, fmt.Sprint(id), numberAt(t, loadedJSON, "_id").String(),
		"a key at /_id must round-trip exactly through the _flow_id rename")
}

// TestStoreDocumentIdFlowIdCollisionReturnsError is the regression test for the
// _id/_flow_id collision clobbering a restored key: when a document has both a
// literal _id field and a key field literally named _flow_id, the _id-collision
// rename has nowhere safe to put the displaced _id value without destroying the
// key. storeDocument must fail loudly rather than silently let the rename
// overwrite the restored key with the unrelated _id value.
func TestStoreDocumentIdFlowIdCollisionReturnsError(t *testing.T) {
	const id int64 = 9223372036854775807
	raw := json.RawMessage(fmt.Sprintf(`{"_id":"arbitrary","_flow_id":%d}`, id))

	_, err := storeDocument(raw, "00", false,
		tuple.Tuple{id}, []keyRestoration{{tupleIndex: 0, tokens: []string{"_flow_id"}}})
	require.Error(t, err)
}

// TestStoreDocumentCompositeMixedKey verifies positional correspondence between
// the key tuple and key pointers, and that only the int64 key element is
// overwritten (the string key and non-key number are left as decoded).
func TestStoreDocumentCompositeMixedKey(t *testing.T) {
	const id int64 = 9223372036854775807
	raw := json.RawMessage(fmt.Sprintf(`{"id":%d,"name":"abc","n":5}`, id))

	doc, loadedJSON := storeAndReload(t, raw, "00", false,
		tuple.Tuple{id, "abc"}, []string{"/id", "/name"})

	require.IsType(t, int64(0), doc["id"], "int64 key element is overwritten exactly")
	require.Equal(t, "abc", doc["name"], "string key element is left untouched")
	require.IsType(t, float64(0), doc["n"], "non-key integer stays float64")

	require.Equal(t, fmt.Sprint(id), numberAt(t, loadedJSON, "id").String(),
		"int64 key round-trips exactly")
}

// TestStoreDocumentDeltaUpdatesLeavesKeyAsDouble verifies that delta-update
// bindings skip int64 key restoration. They never load documents and let MongoDB
// generate the _id, so restoring precision serves no purpose and would needlessly
// flip an integer key field's BSON type from double to long on existing
// collections.
func TestStoreDocumentDeltaUpdatesLeavesKeyAsDouble(t *testing.T) {
	const id int64 = 9223372036854775807
	raw := json.RawMessage(fmt.Sprintf(`{"id":%d}`, id))

	doc, _ := storeAndReload(t, raw, "00", true,
		tuple.Tuple{id}, []string{"/id"})

	require.IsType(t, float64(0), doc["id"],
		"delta-update key field must stay float64 (BSON type unchanged)")
	_, hasID := doc[idField]
	require.False(t, hasID, "delta updates must not set _id")
}

// TestStoreDocumentKeyIndexOutOfBoundsReturnsError asserts that a
// keyRestoration whose tupleIndex falls outside the store's key tuple — an
// impossible state, since the runtime packs exactly one key element per
// declared key field — fails loudly rather than silently storing a document
// with un-restored key precision.
func TestStoreDocumentKeyIndexOutOfBoundsReturnsError(t *testing.T) {
	raw := json.RawMessage(`{"id":1}`)
	_, err := storeDocument(raw, "00", false,
		tuple.Tuple{int64(1)}, []keyRestoration{{tupleIndex: 5, tokens: []string{"id"}}})
	require.Error(t, err)
}

// TestStoreDocumentKeyPointerUnresolvedReturnsError asserts that storeDocument
// fails loudly, rather than silently storing a document with un-restored key
// precision, when a key field's declared JSON pointer doesn't resolve in the
// document body (here, the intermediate object "a" is missing entirely).
func TestStoreDocumentKeyPointerUnresolvedReturnsError(t *testing.T) {
	raw := json.RawMessage(`{}`)
	_, err := storeDocument(raw, "00", false,
		tuple.Tuple{int64(1)}, []keyRestoration{{tupleIndex: 0, tokens: []string{"a", "id"}}})
	require.Error(t, err)
}

// TestSanitizeDocumentInnerArrayNaN is the regression test for NaN nested
// inside an array surviving load-side sanitization: the mongo-driver decodes a
// loaded document's nested BSON arrays as primitive.A (bson.A), never
// []interface{}, so a NaN nested inside an array element — e.g.
// {"items": [{"val": NaN}]} — passed through sanitizeDocumentInner untouched
// and broke the subsequent json.Marshal of the loaded document. The array is
// round-tripped through bson.Marshal/Unmarshal so the test exercises the
// driver-authentic primitive.A shape rather than a hand-built []interface{}.
func TestSanitizeDocumentInnerArrayNaN(t *testing.T) {
	encoded, err := bson.Marshal(bson.M{"items": bson.A{bson.M{"val": math.NaN()}}})
	require.NoError(t, err)
	var doc bson.M
	require.NoError(t, bson.Unmarshal(encoded, &doc))

	sanitized := sanitizeDocumentInner(doc)
	_, err = json.Marshal(sanitized)
	require.NoError(t, err, "NaN nested inside an array must be sanitized before marshaling")
}

// TestSanitizeDocumentInnerNestedObjectNaN is the regression test for the
// sibling gap to TestSanitizeDocumentInnerArrayNaN: the mongo-driver decodes a
// loaded document's nested sub-objects as primitive.M (bson.M), never plain
// map[string]interface{}, so a NaN nested inside a sub-object — not even
// inside an array — also passed through sanitizeDocumentInner untouched.
func TestSanitizeDocumentInnerNestedObjectNaN(t *testing.T) {
	encoded, err := bson.Marshal(bson.M{"obj": bson.M{"val": math.NaN()}})
	require.NoError(t, err)
	var doc bson.M
	require.NoError(t, bson.Unmarshal(encoded, &doc))

	sanitized := sanitizeDocumentInner(doc)
	_, err = json.Marshal(sanitized)
	require.NoError(t, err, "NaN nested inside a sub-object must be sanitized before marshaling")
}

// BenchmarkStoreDocumentCompositeKey measures storeDocument's restore loop for
// a composite key with one integer field (eligible for restoration) and two
// non-integer fields (string, boolean) that keyRestorations excludes
// up front. It exists to confirm the eligibility filter and root-level fast
// path keep this loop's cost close to the single-key case rather than scaling
// with the binding's total key-field count.
func BenchmarkStoreDocumentCompositeKey(b *testing.B) {
	const id int64 = 9223372036854775807
	raw := json.RawMessage(fmt.Sprintf(`{"id":%d,"code":"abc","active":true,"name":"n"}`, id))
	key := tuple.Tuple{id, "abc", true}
	// Only "id" is integer-typed; driver.go's isIntegerKeyType filter would
	// exclude "code" and "active" from keyRestorations entirely.
	restorations := []keyRestoration{{tupleIndex: 0, tokens: []string{"id"}}}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := storeDocument(raw, "00", false, key, restorations); err != nil {
			b.Fatal(err)
		}
	}
}

// bulkErr builds a mongo.BulkWriteException with one per-item WriteError per
// entry in codeByIndex (index -> MongoDB error code). It is returned by value,
// which satisfies the error interface.
func bulkErr(codeByIndex map[int]int) mongo.BulkWriteException {
	var bwe mongo.BulkWriteException
	for idx, code := range codeByIndex {
		bwe.WriteErrors = append(bwe.WriteErrors, mongo.BulkWriteError{
			WriteError: mongo.WriteError{Index: idx, Code: code, Message: fmt.Sprintf("code %d", code)},
		})
	}
	return bwe
}

func model(id string) mongo.WriteModel {
	return &mongo.InsertOneModel{Document: bson.M{idField: id}}
}

func noDelay(context.Context, int) error { return nil }

// TestStoreBulkRetryResendsOnlyFailedModels is the regression test for transient
// per-item bulk write errors: a retryable per-item error (e.g. PrimarySteppedDown,
// code 189) must be retried by re-sending only the failed model, not surfaced as
// fatal. Counts from each attempt accumulate so the caller's sanity check holds.
func TestStoreBulkRetryResendsOnlyFailedModels(t *testing.T) {
	models := []mongo.WriteModel{model("A"), model("B"), model("C")}

	responses := []struct {
		res *mongo.BulkWriteResult
		err error
	}{
		{&mongo.BulkWriteResult{InsertedCount: 2}, bulkErr(map[int]int{1: 189})}, // B fails transiently
		{&mongo.BulkWriteResult{MatchedCount: 1}, nil},                           // resent B succeeds
	}
	var sent [][]mongo.WriteModel
	send := func(models []mongo.WriteModel) (*mongo.BulkWriteResult, error) {
		sent = append(sent, models)
		r := responses[0]
		responses = responses[1:]
		return r.res, r.err
	}

	res, err := storeBulkWithRetry(context.Background(), models, maxStoreRetries, send, noDelay)
	require.NoError(t, err)
	require.Len(t, sent, 2, "should send the initial batch then a retry")
	require.Len(t, sent[0], 3, "first send is the whole batch")
	require.Len(t, sent[1], 1, "retry re-sends only the failed model")
	require.Same(t, models[1], sent[1][0], "retry re-sends exactly the failed model")
	require.Equal(t, int64(2), res.InsertedCount, "counts accumulate across attempts")
	require.Equal(t, int64(1), res.MatchedCount, "counts accumulate across attempts")
}

// TestStoreBulkRetryFailsFastOnDuplicateKey guards the strict-insert tripwire: an
// E11000 duplicate-key (code 11000) is non-retryable and must be returned
// immediately without retrying.
func TestStoreBulkRetryFailsFastOnDuplicateKey(t *testing.T) {
	models := []mongo.WriteModel{model("A")}

	var sends int
	send := func(models []mongo.WriteModel) (*mongo.BulkWriteResult, error) {
		sends++
		return &mongo.BulkWriteResult{}, bulkErr(map[int]int{0: 11000})
	}

	_, err := storeBulkWithRetry(context.Background(), models, maxStoreRetries, send, noDelay)
	require.Error(t, err)
	require.Equal(t, 1, sends, "a duplicate-key error must not be retried")
	var bwe mongo.BulkWriteException
	require.True(t, errors.As(err, &bwe), "terminal error should be the BulkWriteException")
	require.Equal(t, 11000, bwe.WriteErrors[0].Code)
}

// TestStoreBulkRetryExhaustsAttempts guards that a persistently retryable error
// gives up after maxAttempts rather than looping forever, surfacing the
// underlying error.
func TestStoreBulkRetryExhaustsAttempts(t *testing.T) {
	models := []mongo.WriteModel{model("A")}

	var sends int
	send := func(models []mongo.WriteModel) (*mongo.BulkWriteResult, error) {
		sends++
		return &mongo.BulkWriteResult{}, bulkErr(map[int]int{0: 10107}) // NotWritablePrimary
	}

	_, err := storeBulkWithRetry(context.Background(), models, 3 /* maxAttempts */, send, noDelay)
	require.Error(t, err)
	require.Equal(t, 3, sends, "should attempt exactly maxAttempts times")
}
