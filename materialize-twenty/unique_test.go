package connector

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Unique values moving between records: swaps, chains and moves across chunks.

func domainOf(h *harness, key string) any {
	return fakeGet(h.only("companies", key), "domainName.primaryLinkUrl")
}

func domains(h *harness, keys ...string) []any {
	var out []any
	for _, k := range keys {
		out = append(out, domainOf(h, k))
	}
	return out
}

func seedDomain(h *harness, key, domain string) {
	h.fake.seed("companies", record{"externalId": key, "domainName": map[string]any{"primaryLinkUrl": domain}})
}

var blogLinks = []any{map[string]any{"url": "https://blog.example", "label": "Blog"}}

// seedLink seeds a company whose domain has a label and a secondary link, as a person set them.
func seedLink(h *harness, key, domain string) {
	h.fake.seed("companies", record{"externalId": key, "domainName": map[string]any{
		"primaryLinkUrl": domain, "primaryLinkLabel": "Label " + key, "secondaryLinks": blogLinks}})
}

// requireLinks checks that a company kept the label and secondary link seedLink gave it.
func requireLinks(t *testing.T, h *harness, key string) {
	t.Helper()
	link := h.only("companies", key)["domainName"].(map[string]any)
	require.Equal(t, "Label "+key, link["primaryLinkLabel"], key)
	require.Equal(t, blogLinks, link["secondaryLinks"], key)
}

// rejectBad makes Twenty reject a record named "Bad!".
func rejectBad(_ string, rec record) string {
	if rec["name"] == "Bad!" {
		return "Bad"
	}
	return ""
}

// stopAtBatch makes the nth batch write to companies, and those after it, fail as when the task
// stops.
func stopAtBatch(h *harness, n int) {
	posts := 0
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/rest/batch/companies") {
			if posts++; posts >= n {
				h.fake.failures = append(h.fake.failures, fakeFailure{method: http.MethodPost, path: "/rest/batch/companies", status: 401, body: messagesBody("Token invalid.")})
			}
		}
	}
}

var uniqueModes = []string{"", `,"unique_conflict":"fail"`}

func TestStoreUniqueSwap(t *testing.T) {
	for _, mode := range uniqueModes {
		h := newHarness(t, companies(mode))
		h.fake.seed("companies", record{"externalId": "a", "domainName": map[string]any{"primaryLinkUrl": "x.com", "primaryLinkLabel": "A"}})
		seedDomain(h, "b", "y.com")
		// A replay of the same documents finds the values moved and writes them again.
		for range 3 {
			require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`)), mode)
			require.Empty(t, h.stats("companies").skipped, mode)
			require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "b"), mode)
		}
		require.Equal(t, "A", fakeGet(h.only("companies", "a"), "domainName.primaryLinkLabel"), "the label stays with its record")
		require.Equal(t, 2, h.fake.rowCount("companies"), mode)
	}
}

func TestStoreUniqueChainOfThree(t *testing.T) {
	// a: x -> y, b: y -> z, c: z -> w.
	for _, mode := range uniqueModes {
		h := newHarness(t, companies(mode))
		seedDomain(h, "a", "x.com")
		seedDomain(h, "b", "y.com")
		seedDomain(h, "c", "z.com")
		require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"z.com"}`), h.doc(0, `{"id":"c","domain":"w.com"}`)), mode)
		require.Equal(t, stats{received: 3, updated: 3}, h.stats("companies"), mode)
		require.Equal(t, []any{"y.com", "z.com", "w.com"}, domains(h, "a", "b", "c"), mode)
		// c takes w.com at once. Then b alone gives up y.com, since no record takes a's x.com, and
		// a and b take their values.
		require.Equal(t, []int{3, 1, 2}, h.fake.batches, mode)
	}
}

func TestStoreUniqueChainThroughMovingRecord(t *testing.T) {
	// a: x -> y, b: y -> z, and a new c takes x, which a gives up while its own value waits for b.
	for _, mode := range uniqueModes {
		h := newHarness(t, companies(mode))
		seedDomain(h, "a", "x.com")
		seedDomain(h, "b", "y.com")
		for range 2 {
			require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"z.com"}`),
				h.doc(0, `{"id":"c","name":"C","domain":"x.com"}`)), mode)
			require.Empty(t, h.stats("companies").skipped, mode)
			require.Equal(t, []any{"y.com", "z.com", "x.com"}, domains(h, "a", "b", "c"), mode)
		}
		require.Equal(t, 3, h.fake.rowCount("companies"), mode)
	}
}

func TestStoreUniqueChainBlocked(t *testing.T) {
	// a: x -> y, b: y -> z, c: z -> w, but o holds w and has no document. Nothing can move, so
	// each record keeps its value.
	for _, mode := range uniqueModes {
		h := newHarness(t, companies(mode))
		seedDomain(h, "a", "x.com")
		seedDomain(h, "b", "y.com")
		seedDomain(h, "c", "z.com")
		seedDomain(h, "o", "w.com")
		for range 2 {
			err := h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"z.com"}`), h.doc(0, `{"id":"c","domain":"w.com"}`))
			if mode == "" {
				require.NoError(t, err)
				require.Equal(t, map[string]int{"domainName_held_by_another_record": 3}, h.stats("companies").skipped)
			} else {
				require.ErrorContains(t, err, "belongs to another record")
			}
			require.Equal(t, []any{"x.com", "y.com", "z.com", "w.com"}, domains(h, "a", "b", "c", "o"), mode)
		}
	}
}

func TestStoreUniqueBlockedMoveKeepsLinks(t *testing.T) {
	// a: x -> y, b: y -> z, o holds z, and a new c takes x. b can't move, so neither can a, and a
	// keeps x.com with its label and secondary link.
	h := newHarness(t, companies(""))
	seedLink(h, "a", "x.com")
	seedDomain(h, "b", "y.com")
	seedDomain(h, "o", "z.com")
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"z.com"}`),
		h.doc(0, `{"id":"c","name":"C","domain":"x.com"}`)))
	require.Equal(t, map[string]int{"domainName_held_by_another_record": 3}, h.stats("companies").skipped)
	require.Equal(t, []any{"x.com", "y.com", nil, "z.com"}, domains(h, "a", "b", "c", "o"))
	requireLinks(t, h, "a")
}

func TestStoreUniqueSwapPartnerRejected(t *testing.T) {
	// b's document is rejected, so b keeps y.com, and a, which can't take it, keeps x.com.
	h := newHarness(t, companies(`,"rejected_records":"skip"`))
	h.fake.reject = rejectBad
	seedLink(h, "a", "x.com")
	seedDomain(h, "b", "y.com")
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","name":"Bad!","domain":"x.com"}`)))
	require.Equal(t, 1, h.stats("companies").rejected)
	require.Equal(t, map[string]int{"domainName_held_by_another_record": 1}, h.stats("companies").skipped)
	require.Equal(t, []any{"x.com", "y.com"}, domains(h, "a", "b"))
	requireLinks(t, h, "a")
}

func TestStoreUniqueStandInRejected(t *testing.T) {
	// Twenty refuses b's stand-in, so b keeps y.com, and a takes x.com back, here and when the
	// transaction settles. The task goes on: neither record's document was rejected.
	for _, mode := range uniqueModes {
		h := newHarness(t, companies(mode))
		h.fake.reject = func(_ string, rec record) string {
			if rec["externalId"] == "b" && fakeGet(rec, "domainName.primaryLinkUrl") == "" {
				return "Bad"
			}
			return ""
		}
		seedDomain(h, "a", "x.com")
		seedDomain(h, "b", "y.com")
		err := h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`))
		if mode == "" {
			require.NoError(t, err)
			require.Equal(t, map[string]int{"domainName_held_by_another_record": 2}, h.stats("companies").skipped)
		} else {
			require.ErrorContains(t, err, "belongs to another record")
		}
		require.Equal(t, []any{"x.com", "y.com"}, domains(h, "a", "b"), mode)
	}
}

func TestStoreUniqueReplayKeepsLinks(t *testing.T) {
	// The task stops after the swap's stand-ins are written. They keep each record's label and
	// secondary link, so the replay moves the domains and loses neither.
	h := newHarness(t, companies(""))
	seedLink(h, "a", "x.com")
	seedLink(h, "b", "y.com")
	swap := []storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`)}
	stopAtBatch(h, 3) // the records, their stand-ins, then their values
	require.Error(t, h.store(swap...))
	h.fake.hook = nil
	for _, k := range []string{"a", "b"} {
		require.Equal(t, h.only("companies", k)["id"].(string)+".invalid", domainOf(h, k))
		requireLinks(t, h, k)
	}
	require.NoError(t, h.store(swap...))
	require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "b"))
	requireLinks(t, h, "a")
	requireLinks(t, h, "b")

	// A record without a label or secondary links holds an empty stand-in.
	h = newHarness(t, companies(""))
	seedDomain(h, "a", "x.com")
	seedDomain(h, "b", "y.com")
	swap = []storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`)}
	stopAtBatch(h, 3)
	require.Error(t, h.store(swap...))
	h.fake.hook = nil
	require.Equal(t, []any{"", ""}, domains(h, "a", "b"))
	require.NoError(t, h.store(swap...))
	require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "b"))

	// The same across chunks, where settleUnique writes the stand-ins.
	h = newHarness(t, companies(""))
	seedLink(h, "a", "x.com")
	seedLink(h, "z", "y.com")
	items := append([]storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`)}, filler(h, "m", maxBatch-1)...)
	items = append(items, h.doc(0, `{"id":"z","domain":"x.com"}`))
	stopAtBatch(h, 5) // the fillers, a, z, the stand-ins, then the values
	require.Error(t, h.store(items...))
	h.fake.hook = nil
	require.NoError(t, h.store(items...))
	require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "z"))
	requireLinks(t, h, "a")
	requireLinks(t, h, "z")
}

// filler is n documents without unique values, keyed prefix00 and up, to push others into
// another chunk. Flow sends a binding's documents ordered by key.
func filler(h *harness, prefix string, n int) []storeItem {
	var items []storeItem
	for i := range n {
		items = append(items, h.doc(0, fmt.Sprintf(`{"id":"%s%02d","name":"Filler"}`, prefix, i)))
	}
	return items
}

func TestStoreUniqueMoveAcrossChunks(t *testing.T) {
	for _, mode := range uniqueModes {
		// a takes y.com in the first chunk; z gives it up in the second.
		h := newHarness(t, companies(mode))
		seedDomain(h, "a", "x.com")
		seedDomain(h, "z", "y.com")
		items := append([]storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`)}, filler(h, "m", maxBatch-1)...)
		require.NoError(t, h.store(append(items, h.doc(0, `{"id":"z","domain":"free.com"}`))...), mode)
		require.Empty(t, h.stats("companies").skipped, mode)
		require.Equal(t, []any{"y.com", "free.com"}, domains(h, "a", "z"), mode)

		// A swap across chunks.
		h = newHarness(t, companies(mode))
		seedDomain(h, "a", "x.com")
		seedDomain(h, "z", "y.com")
		items = append([]storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`)}, filler(h, "m", maxBatch-1)...)
		require.NoError(t, h.store(append(items, h.doc(0, `{"id":"z","domain":"x.com"}`))...), mode)
		require.Empty(t, h.stats("companies").skipped, mode)
		require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "z"), mode)
		require.Equal(t, maxBatch+1, h.fake.rowCount("companies"), mode)
	}
}

func TestStoreUniqueLinkAfterMove(t *testing.T) {
	// a and z swap domains across chunks, and Jane's company is y.com: a's once the companies are
	// written, so her link, resolved after them, finds a.
	h := newHarness(t, companies(""), people(`,"relations":[{"field":"company","id_field":"domainName"}]`))
	h.clock()
	seedDomain(h, "a", "x.com")
	seedDomain(h, "z", "y.com")
	items := append([]storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`)}, filler(h, "m", maxBatch-1)...)
	items = append(items, h.doc(0, `{"id":"z","domain":"x.com"}`), h.doc(1, `{"id":1,"first":"Jane","company":"y.com"}`))
	require.NoError(t, h.store(items...))
	require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "z"))
	require.Equal(t, h.only("companies", "a")["id"], h.only("people", "1")["companyId"])

	// A saved link tried again after the move finds a too.
	h = newHarness(t, companies(""), people(`,"relations":[{"field":"company","id_field":"domainName"}]`))
	h.clock()
	require.NoError(t, h.store(h.doc(1, `{"id":1,"first":"Jane","company":"y.com"}`)))
	require.Nil(t, h.only("people", "1")["companyId"])
	seedDomain(h, "a", "x.com")
	seedDomain(h, "z", "y.com")
	h.now = h.now.Add(linkRetryEvery)
	items = append([]storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`)}, filler(h, "m", maxBatch-1)...)
	require.NoError(t, h.store(append(items, h.doc(0, `{"id":"z","domain":"x.com"}`))...))
	require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "z"))
	require.Equal(t, 1, h.stats("people").linkedLater)
	require.Equal(t, h.only("companies", "a")["id"], h.only("people", "1")["companyId"])
}

func TestStoreUniqueRejectedRecordTakesNoValueLater(t *testing.T) {
	// a's document is rejected, so a doesn't take y.com when z gives it up in a later chunk.
	h := newHarness(t, companies(`,"rejected_records":"skip"`))
	h.fake.reject = rejectBad
	seedDomain(h, "a", "x.com")
	seedDomain(h, "z", "y.com")
	items := append([]storeItem{h.doc(0, `{"id":"a","name":"Bad!","domain":"y.com"}`)}, filler(h, "m", maxBatch-1)...)
	require.NoError(t, h.store(append(items, h.doc(0, `{"id":"z","domain":"free.com"}`))...))
	require.Equal(t, 1, h.stats("companies").rejected)
	require.Equal(t, []any{"x.com", "free.com"}, domains(h, "a", "z"))
}

func TestStoreUniqueMoveSkipsRecordDeletedMeanwhile(t *testing.T) {
	// Someone moves a to Twenty's trash before the transaction's moves are written, so a stays
	// there and doesn't take y.com.
	h := newHarness(t, companies(""))
	seedDomain(h, "a", "x.com")
	seedDomain(h, "z", "y.com")
	posts := 0
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/rest/batch/companies") {
			// The fillers, a, then z.
			if posts++; posts == 3 {
				for _, row := range h.fake.rows["companies"] {
					if row["externalId"] == "a" {
						row["deletedAt"] = "2026-11-10T00:00:00.000Z"
					}
				}
			}
		}
	}
	items := append([]storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`)}, filler(h, "m", maxBatch-1)...)
	require.NoError(t, h.store(append(items, h.doc(0, `{"id":"z","domain":"free.com"}`))...))
	a := h.only("companies", "a")
	require.NotNil(t, a["deletedAt"])
	require.Equal(t, "x.com", fakeGet(a, "domainName.primaryLinkUrl"))
}

// The tests below send a key twice in one transaction, across a chunk boundary. Flow reduces a
// key's documents within a transaction, so it never does; they check that the connector's own
// bookkeeping holds up if it did.

func TestStoreUniquePostponedThenSuperseded(t *testing.T) {
	for _, mode := range uniqueModes {
		// a's y.com waits for the end of the transaction, but a later document of a sets another
		// domain, and o never gives y.com up.
		h := newHarness(t, companies(mode))
		seedDomain(h, "o", "y.com")
		items := append(filler(h, "0-", maxBatch-1), h.doc(0, `{"id":"a","name":"A","domain":"y.com"}`))
		require.NoError(t, h.store(append(items, h.doc(0, `{"id":"a","domain":"w.com"}`))...), mode)
		require.Empty(t, h.stats("companies").skipped, mode)
		require.Equal(t, []any{"w.com", "y.com"}, domains(h, "a", "o"), mode)

		// A later deletion drops it too, so a record flagged as deleted doesn't take the value
		// when z gives it up.
		for _, del := range []string{`,"delete_mode":"delete"`, flagDeletes} {
			h = newHarness(t, companies(mode+del))
			seedDomain(h, "z", "y.com")
			items = append(filler(h, "0-", maxBatch-1), h.doc(0, `{"id":"a","name":"A","domain":"y.com"}`))
			require.NoError(t, h.store(append(items, h.deletion(0, "a"), h.doc(0, `{"id":"z","domain":"free.com"}`))...), mode+del)
			if del == flagDeletes {
				require.Nil(t, domainOf(h, "a"), mode+del)
			} else {
				require.Empty(t, h.fake.byKey("companies", "a"), mode+del)
			}
			require.Equal(t, "free.com", domainOf(h, "z"), mode+del)
		}
	}
}

func TestStoreUniqueLaterDocumentRejected(t *testing.T) {
	// k's first document waits for y.com, which z gives up. k's second document is rejected, which
	// leaves the first as written, y.com included.
	h := newHarness(t, companies(`,"rejected_records":"skip"`))
	h.fake.reject = rejectBad
	seedDomain(h, "z", "y.com")
	items := append(filler(h, "a", maxBatch-1), h.doc(0, `{"id":"k","name":"K","domain":"y.com"}`), h.doc(0, `{"id":"k","name":"Bad!"}`),
		h.doc(0, `{"id":"z","domain":"free.com"}`))
	require.NoError(t, h.store(items...))
	require.Equal(t, 1, h.stats("companies").rejected)
	require.Equal(t, "K", h.only("companies", "k")["name"])
	require.Equal(t, []any{"y.com", "free.com"}, domains(h, "k", "z"))
}

func TestStoreUniqueHeldOutsideTransaction(t *testing.T) {
	// A value no document gives up is left out once, at the end of the transaction.
	h := newHarness(t, companies(""))
	seedDomain(h, "o", "y.com")
	items := append([]storeItem{h.doc(0, `{"id":"a","name":"A","domain":"y.com"}`)}, filler(h, "m", maxBatch-1)...)
	require.NoError(t, h.store(append(items, h.doc(0, `{"id":"n","name":"N","domain":"y.com"}`))...))
	require.Equal(t, map[string]int{"domainName_held_by_another_record": 2}, h.stats("companies").skipped)
	require.Equal(t, []any{nil, nil, "y.com"}, domains(h, "a", "n", "o"))
	require.Equal(t, "A", h.only("companies", "a")["name"])

	h = newHarness(t, companies(`,"unique_conflict":"fail"`))
	seedDomain(h, "o", "y.com")
	for range 2 {
		require.ErrorContains(t, h.store(h.doc(0, `{"id":"a","name":"A","domain":"y.com"}`)),
			`the domainName value of the document with key "a" belongs to another record`)
	}
	require.Equal(t, "y.com", domainOf(h, "o"))
}

func TestStoreUniqueDeletedRecordKeepsItsValue(t *testing.T) {
	// The source deletes k1 and makes k2 with the same domain, as a delete-and-recreate does.
	replace := func(h *harness) error {
		require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Old","domain":"acme.com"}`)))
		return h.store(h.deletion(0, "k1"), h.doc(0, `{"id":"k2","name":"New","domain":"acme.com"}`))
	}
	for _, del := range []string{"", `,"delete_mode":"trash"`, flagDeletes} {
		// The old record still holds the domain, live, flagged or in the trash.
		h := newHarness(t, companies(del))
		require.NoError(t, replace(h), del)
		require.Equal(t, 1, h.stats("companies").skipped["domainName_held_by_another_record"], del)
		require.Nil(t, domainOf(h, "k2"), del)

		h = newHarness(t, companies(del+`,"unique_conflict":"fail"`))
		require.ErrorContains(t, replace(h), `the domainName value of the document with key "k2" belongs to another record`, del)
	}
	// Only a permanent delete frees it.
	for _, mode := range uniqueModes {
		h := newHarness(t, companies(mode+`,"delete_mode":"delete"`))
		require.NoError(t, replace(h), mode)
		require.Equal(t, "acme.com", domainOf(h, "k2"), mode)
	}
}
