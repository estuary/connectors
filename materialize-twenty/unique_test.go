package connector

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
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
		// c takes w.com at once. Then b alone gives up y.com, since no record takes a's x.com: it
		// writes y.com as it is, then its stand-in, and a and b take their values.
		require.Equal(t, []int{3, 1, 1, 2}, h.fake.batches, mode)
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
			if u, _ := fakeGet(rec, "domainName.primaryLinkUrl").(string); rec["externalId"] == "b" && strings.HasSuffix(u, ".invalid") {
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
	// secondary link, and the next transaction gives the records their values back before it
	// moves them, so neither is lost.
	h := newHarness(t, companies(""))
	seedLink(h, "a", "x.com")
	seedLink(h, "b", "y.com")
	swap := []storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`)}
	stopAtBatch(h, 4) // the records, their values as they are, their stand-ins, then their values
	require.Error(t, h.store(swap...))
	h.fake.hook = nil
	for _, k := range []string{"a", "b"} {
		require.Equal(t, fmt.Sprintf("%s.%s.invalid", map[string]string{"a": "x.com", "b": "y.com"}[k], h.only("companies", k)["id"]), domainOf(h, k))
		requireLinks(t, h, k)
	}
	require.NoError(t, h.store(swap...))
	require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "b"))
	requireLinks(t, h, "a")
	requireLinks(t, h, "b")

	// A record without a label or secondary links holds one too, and a record whose value
	// another record took since holds none.
	h = newHarness(t, companies(""))
	seedDomain(h, "a", "x.com")
	seedDomain(h, "b", "y.com")
	swap = []storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`)}
	stopAtBatch(h, 4)
	require.Error(t, h.store(swap...))
	h.fake.hook = nil
	require.Equal(t, fmt.Sprintf("x.com.%s.invalid", h.only("companies", "a")["id"]), domainOf(h, "a"))
	seedDomain(h, "o", "y.com")
	require.NoError(t, h.store(h.doc(0, `{"id":"a","name":"A"}`)))
	require.Equal(t, []any{"x.com", "", "y.com"}, domains(h, "a", "b", "o"))

	// The same across chunks, where settleUnique writes the stand-ins.
	h = newHarness(t, companies(""))
	seedLink(h, "a", "x.com")
	seedLink(h, "z", "y.com")
	items := append([]storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`)}, filler(h, "m", maxBatch-1)...)
	items = append(items, h.doc(0, `{"id":"z","domain":"x.com"}`))
	stopAtBatch(h, 6) // the fillers, a, z, the values as they are, the stand-ins, then the values
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

// withProjections adds projections to a binding's collection.
func withProjections(tb testBinding, ps ...pf.Projection) testBinding {
	c := tb.collection
	c.Projections = append(slices.Clone(c.Projections), ps...)
	slices.SortFunc(c.Projections, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })
	tb.collection = c
	return tb
}

func TestStoreUniqueReplayAfterValueClaimed(t *testing.T) {
	// The task stops after s and t, which swap domains, hold their stand-ins. In the replay, a in
	// an earlier chunk wants y.com, and finds it on t again rather than free, so s doesn't lose
	// x.com to t without taking y.com.
	for _, labeled := range []bool{false, true} {
		h := newHarness(t, companies(""))
		h.fake.seed("companies", record{"externalId": "a", "name": "A"})
		if labeled {
			seedLink(h, "s", "x.com")
		} else {
			seedDomain(h, "s", "x.com")
		}
		seedDomain(h, "t", "y.com")
		items := append([]storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`)}, filler(h, "b", maxBatch-1)...)
		items = append(items, h.doc(0, `{"id":"s","domain":"y.com"}`), h.doc(0, `{"id":"t","domain":"x.com"}`))
		stopAtBatch(h, 6) // the fillers, a, s and t, their values as they are, their stand-ins, then their values
		require.Error(t, h.store(items...))
		require.Contains(t, domainOf(h, "s"), ".invalid")
		h.fake.hook = nil
		require.NoError(t, h.store(items...))
		require.Equal(t, []any{nil, "y.com", "x.com"}, domains(h, "a", "s", "t"), labeled)
		require.Equal(t, map[string]int{"domainName_held_by_another_record": 1}, h.stats("companies").skipped, labeled)
	}
}

func TestStoreUniqueReplayWithoutField(t *testing.T) {
	// The task stops after a and b, which swap domains, hold their stand-ins. The replay's newer
	// document of b sets no domain, so b takes y.com back and a keeps x.com.
	h := newHarness(t, companies(""))
	seedLink(h, "a", "x.com")
	seedLink(h, "b", "y.com")
	stopAtBatch(h, 4)
	require.Error(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`)))
	h.fake.hook = nil
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","name":"B"}`)))
	require.Equal(t, []any{"x.com", "y.com"}, domains(h, "a", "b"))
	requireLinks(t, h, "a")
	requireLinks(t, h, "b")
}

func TestStoreUniqueFinalWriteRefused(t *testing.T) {
	// b holds a domain from before Twenty checked domains, and Twenty refuses it as a's new value,
	// and as b's old one, though it may take b's stand-in. Neither record gives its value up, and
	// the task goes on, as for any conflict.
	for _, old := range []string{"y_com.example", "localhost"} {
		for _, rejected := range []string{`,"rejected_records":"skip"`, ""} {
			h := newHarness(t, companies(rejected))
			seedLink(h, "a", "x.com")
			seedDomain(h, "b", old)
			for range 2 {
				require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"`+old+`"}`), h.doc(0, `{"id":"b","domain":"x.com"}`)), old+rejected)
				require.Equal(t, map[string]int{"domainName_held_by_another_record": 2}, h.stats("companies").skipped, old+rejected)
				require.Equal(t, []any{"x.com", old}, domains(h, "a", "b"), old+rejected)
				requireLinks(t, h, "a")
			}
		}
	}

	// Twenty refuses only the final write: b takes a's x.com, which a rewrote and gave up, but not
	// y.com, which someone else took meanwhile. a takes x.com back.
	h := newHarness(t, companies(""))
	seedLink(h, "a", "x.com")
	seedDomain(h, "b", "y.com")
	h.fake.reject = func(_ string, rec record) string {
		if rec["externalId"] == "a" && fakeGet(rec, "domainName.primaryLinkUrl") == "y.com" {
			return "Bad"
		}
		return ""
	}
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`)))
	require.Equal(t, []any{"x.com", "y.com"}, domains(h, "a", "b"))
	requireLinks(t, h, "a")

	// Someone takes x.com while a holds its stand-in, so the move fails and a holds no domain
	// rather than its stand-in. b takes y.com back.
	h = newHarness(t, companies(""))
	seedLink(h, "a", "x.com")
	seedDomain(h, "b", "y.com")
	posts := 0
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/rest/batch/companies") {
			if posts++; posts == 4 {
				id := h.fake.newID()
				row := h.fake.blank("companies")
				merge(row, record{"id": id, "domainName": map[string]any{"primaryLinkUrl": "x.com"}})
				h.fake.rows["companies"][id] = row
			}
		}
	}
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`)))
	require.Equal(t, []any{"", "y.com"}, domains(h, "a", "b"))
	require.Empty(t, fakeGet(h.only("companies", "a"), "domainName.secondaryLinks"), "an empty domain has no secondary link to take its place")
}

func TestStoreUniqueGroupRefused(t *testing.T) {
	// Twenty refuses a's new value, so a and b keep theirs, and c and d, in the same batch, swap.
	h := newHarness(t, companies(""))
	for k, v := range map[string]string{"a": "x.com", "b": "y.com", "c": "z.com", "d": "w.com"} {
		seedDomain(h, k, v)
	}
	h.fake.reject = func(_ string, rec record) string {
		if rec["externalId"] == "a" && fakeGet(rec, "domainName.primaryLinkUrl") == "y.com" {
			return "Bad"
		}
		return ""
	}
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`),
		h.doc(0, `{"id":"c","domain":"w.com"}`), h.doc(0, `{"id":"d","domain":"z.com"}`)))
	require.Equal(t, []any{"x.com", "y.com", "w.com", "z.com"}, domains(h, "a", "b", "c", "d"))
	require.Equal(t, map[string]int{"domainName_held_by_another_record": 2}, h.stats("companies").skipped)
}

func TestStoreUniqueStandInRefusalNamingField(t *testing.T) {
	// A refused stand-in isn't written again without the field, even when Twenty's message names
	// it, which would move x.com to b while a keeps a stand-in.
	h := newHarness(t, companies(`,"rejected_records":"skip"`))
	h.fake.reject = func(_ string, rec record) string {
		if u, _ := fakeGet(rec, "domainName.primaryLinkUrl").(string); rec["externalId"] == "b" && strings.HasSuffix(u, ".invalid") {
			return `"" is not a domain name, for domain-typed links field "domainName.primaryLinkUrl"`
		}
		return ""
	}
	seedDomain(h, "a", "x.com")
	seedDomain(h, "b", "y.com")
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`)))
	require.Equal(t, []any{"x.com", "y.com"}, domains(h, "a", "b"))
}

func TestStoreUniqueHeldBackLinks(t *testing.T) {
	// A record without a domain holds back all of its document's link, so Twenty doesn't make the
	// secondary link its domain: not when another record keeps the domain, nor before c takes the
	// one a gives up, which keeps no label it never had.
	tb := withProjections(companies(""), proj("domainName/secondaryLinks", "/links", "array"))
	h := newHarness(t, tb)
	seedDomain(h, "o", "taken.com")
	require.NoError(t, h.store(h.doc(0, `{"id":"k","name":"K","domain":"taken.com","links":[{"url":"https://blog.example","label":"Blog"}]}`)))
	require.Nil(t, domainOf(h, "k"))

	h = newHarness(t, tb)
	seedDomain(h, "a", "x.com")
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"free.com"}`),
		h.doc(0, `{"id":"c","name":"C","domain":"x.com","links":[{"url":"https://blog.example","label":"Blog"}]}`)))
	link := h.only("companies", "c")["domainName"].(map[string]any)
	require.Equal(t, "x.com", link["primaryLinkUrl"])
	require.Empty(t, link["primaryLinkLabel"])
	require.Equal(t, []any{map[string]any{"url": "https://blog.example", "label": "Blog"}}, link["secondaryLinks"])
}

func TestStoreUniqueConflictFail(t *testing.T) {
	// A record with no key never gives its domain up, so the task stops before it creates k1, and
	// adopt_by, set afterwards, adopts that record.
	h := newHarness(t, companies(`,"unique_conflict":"fail"`))
	h.fake.seed("companies", record{"name": "Made by a person", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	require.ErrorContains(t, h.store(h.doc(0, `{"id":"k1","name":"Acme","domain":"acme.com"}`)), "belongs to another record")
	require.Equal(t, 1, h.fake.rowCount("companies"))
	h2 := harnessFor(t, h.tx.client, companies(`,"unique_conflict":"fail","adopt_by":"domainName"`))
	h2.fake = h.fake
	require.NoError(t, h2.store(h2.doc(0, `{"id":"k1","name":"Acme","domain":"acme.com"}`)))
	require.Equal(t, 1, h.fake.rowCount("companies"))

	// o, which has a key, may give its value up in a later chunk, so the task stops only after
	// the transaction's other moves, like a and z's swap.
	h = newHarness(t, companies(`,"unique_conflict":"fail"`))
	seedDomain(h, "a", "x.com")
	seedDomain(h, "z", "y.com")
	seedDomain(h, "o", "w.com")
	items := append([]storeItem{h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"c","name":"C","domain":"w.com"}`)}, filler(h, "m", maxBatch-2)...)
	items = append(items, h.doc(0, `{"id":"z","domain":"x.com"}`))
	for range 2 {
		require.ErrorContains(t, h.store(items...), `key "c" belongs to another record`)
		require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "z"))
	}
}

func TestStoreUniqueConflictReads(t *testing.T) {
	// A value a record with no key holds is a conflict at once: a lookup and a find, as before
	// values moved. One a record with a key holds waits for the end of the transaction, where
	// two more reads check whether it moved.
	for holder, want := range map[string]int{"": 2, "o": 4} {
		h := newHarness(t, companies(""))
		require.NoError(t, h.store(h.doc(0, `{"id":"w","name":"W"}`)), "the task's first transaction looks for stand-ins")
		rec := record{"domainName": map[string]any{"primaryLinkUrl": "y.com"}}
		if holder != "" {
			rec["externalId"] = holder
		}
		h.fake.seed("companies", rec)
		before := h.fake.count(http.MethodGet, "/rest/companies")
		require.NoError(t, h.store(h.doc(0, `{"id":"a","name":"A","domain":"y.com"}`)))
		require.Equal(t, want, h.fake.count(http.MethodGet, "/rest/companies")-before, holder)
		require.Equal(t, map[string]int{"domainName_held_by_another_record": 1}, h.stats("companies").skipped, holder)
	}
}

func TestStoreUniquePostponedTwiceForOneKey(t *testing.T) {
	// a's domain waits on b in its chunk, and its account code on zz in a later chunk. Both wait in
	// one record for a, and both move.
	for _, mode := range uniqueModes {
		h := newHarness(t, companies(mode))
		h.fake.seed("companies", record{"externalId": "a", "accountCode": "A1", "domainName": map[string]any{"primaryLinkUrl": "x.com"}})
		seedDomain(h, "b", "y.com")
		h.fake.seed("companies", record{"externalId": "zz", "accountCode": "A2", "domainName": map[string]any{"primaryLinkUrl": "z.com"}})
		items := []storeItem{h.doc(0, `{"id":"a","code":"A2","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"z.com"}`)}
		items = append(items, filler(h, "m", maxBatch-2)...)
		require.NoError(t, h.store(append(items, h.doc(0, `{"id":"zz","code":"A9","domain":"w.com"}`))...), mode)
		require.Empty(t, h.stats("companies").skipped, mode)
		require.Equal(t, []any{"y.com", "z.com", "w.com"}, domains(h, "a", "b", "zz"), mode)
		require.Equal(t, "A2", h.only("companies", "a")["accountCode"], mode)
	}
}

func TestStoreUniqueOtherTypes(t *testing.T) {
	// Swaps of a unique text field, of two fields at once, and of people's emails.
	for _, mode := range uniqueModes {
		h := newHarness(t, companies(mode))
		h.fake.seed("companies", record{"externalId": "a", "accountCode": "A1", "domainName": map[string]any{"primaryLinkUrl": "x.com"}})
		h.fake.seed("companies", record{"externalId": "b", "accountCode": "B1", "domainName": map[string]any{"primaryLinkUrl": "y.com", "primaryLinkLabel": "B"}})
		for range 2 {
			require.NoError(t, h.store(h.doc(0, `{"id":"a","code":"B1","domain":"y.com"}`), h.doc(0, `{"id":"b","code":"A1","domain":"x.com"}`)), mode)
			require.Empty(t, h.stats("companies").skipped, mode)
			require.Equal(t, []any{"B1", "A1"}, []any{h.only("companies", "a")["accountCode"], h.only("companies", "b")["accountCode"]}, mode)
			require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "b"), mode)
			require.Equal(t, "B", fakeGet(h.only("companies", "b"), "domainName.primaryLinkLabel"), mode)
		}

		h = newHarness(t, people(mode))
		h.fake.seed("people", record{"externalId": "1", "emails": map[string]any{"primaryEmail": "a@x.com", "additionalEmails": []any{"a2@x.com"}}})
		h.fake.seed("people", record{"externalId": "2", "emails": map[string]any{"primaryEmail": "b@x.com"}})
		require.NoError(t, h.store(h.doc(0, `{"id":1,"email":"b@x.com"}`), h.doc(0, `{"id":2,"email":"a@x.com"}`)), mode)
		require.Empty(t, h.stats("people").skipped, mode)
		require.Equal(t, "b@x.com", fakeGet(h.only("people", "1"), "emails.primaryEmail"), mode)
		require.Equal(t, []any{"a2@x.com"}, fakeGet(h.only("people", "1"), "emails.additionalEmails"), mode)
		require.Equal(t, "a@x.com", fakeGet(h.only("people", "2"), "emails.primaryEmail"), mode)
	}
}

func TestStoreUniqueLongChain(t *testing.T) {
	// Each of more records than fit in a batch takes the next one's account code, and the last a
	// new one or, in a ring, the first one's: one group of moves, written in parts.
	for _, ring := range []bool{false, true} {
		h := newHarness(t, companies(""))
		n := maxBatch + 5
		var items []storeItem
		for i := range n {
			next := i + 1
			if ring {
				next %= n
			}
			h.fake.seed("companies", record{"externalId": fmt.Sprintf("k%02d", i), "accountCode": fmt.Sprintf("C%02d", i)})
			items = append(items, h.doc(0, fmt.Sprintf(`{"id":"k%02d","code":"C%02d"}`, i, next)))
		}
		require.NoError(t, h.store(items...), ring)
		require.Empty(t, h.stats("companies").skipped, ring)
		for i := range n {
			want := fmt.Sprintf("C%02d", i+1)
			if ring {
				want = fmt.Sprintf("C%02d", (i+1)%n)
			}
			require.Equal(t, want, h.only("companies", fmt.Sprintf("k%02d", i))["accountCode"], ring)
		}
	}
}

func TestStoreUniqueLinksFoundAgain(t *testing.T) {
	// Jane's company is y.com, which a takes from z. Her link, found before the swap, is found
	// again after it, with the people binding first or last.
	rel := people(`,"relations":[{"field":"company","id_field":"domainName"}]`)
	for _, peopleFirst := range []bool{false, true} {
		h, c, p := newHarness(t, companies(""), rel), 0, 1
		if peopleFirst {
			h, c, p = newHarness(t, rel, companies("")), 1, 0
		}
		h.clock()
		seedDomain(h, "a", "x.com")
		seedDomain(h, "z", "y.com")
		jane := h.doc(p, `{"id":1,"first":"Jane","company":"y.com"}`)
		items := []storeItem{h.doc(c, `{"id":"a","domain":"y.com"}`), h.doc(c, `{"id":"z","domain":"x.com"}`)}
		if peopleFirst {
			items = append([]storeItem{jane}, items...)
		} else {
			items = append(items, jane)
		}
		require.NoError(t, h.store(items...), peopleFirst)
		require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "z"), peopleFirst)
		require.Equal(t, h.only("companies", "a")["id"], h.only("people", "1")["companyId"], peopleFirst)
	}

	// The same when a gives y.com up in one chunk and z takes it in another, without a move.
	h := newHarness(t, rel, companies(""))
	h.clock()
	seedDomain(h, "a", "y.com")
	items := []storeItem{h.doc(0, `{"id":1,"first":"Jane","company":"y.com"}`), h.doc(1, `{"id":"a","domain":"free.com"}`)}
	for i := range maxBatch - 1 {
		items = append(items, h.doc(1, fmt.Sprintf(`{"id":"m%02d","name":"Filler"}`, i)))
	}
	require.NoError(t, h.store(append(items, h.doc(1, `{"id":"z","name":"Z","domain":"y.com"}`))...))
	require.Equal(t, []any{"free.com", "y.com"}, domains(h, "a", "z"))
	require.Equal(t, h.only("companies", "z")["id"], h.only("people", "1")["companyId"])

	// And for a company's parent, found in the chunk whose documents swap the domain it's found by.
	tb := withProjections(companies(`,"relations":[{"field":"parentCompany","id_field":"domainName","source_field":"parent"}]`),
		proj("parent", "/parent", "string"))
	h = newHarness(t, tb)
	h.clock()
	seedDomain(h, "a", "x.com")
	seedDomain(h, "b", "y.com")
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"y.com"}`), h.doc(0, `{"id":"b","domain":"x.com"}`),
		h.doc(0, `{"id":"c","name":"C","parent":"x.com"}`)))
	require.Equal(t, []any{"y.com", "x.com"}, domains(h, "a", "b"))
	require.Equal(t, h.only("companies", "b")["id"], h.only("companies", "c")["parentCompanyId"])
}

// TestStoreUniqueFuzz moves random domains and account codes between records in three chunks,
// stops the task at each batch write in turn, replays it, and checks that each record ends with
// its new value or its old one, and that a value is left out only when another record holds it.
// TWENTY_FUZZ_SEEDS runs more seeds than the few that keep the test fast.
func TestStoreUniqueFuzz(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	seeds := 6
	if n, err := strconv.Atoi(os.Getenv("TWENTY_FUZZ_SEEDS")); err == nil {
		seeds = n
	}
	// Documents may want the same value, and one record's stand-in may be refused; or no
	// stand-in is refused; or, also, no two documents want the same value.
	variants := []fuzzVariant{{}, {noRefuse: true}, {noDups: true, noRefuse: true}}
	for seed := range int64(seeds) {
		fv := variants[seed%3]
		msg, posts := fuzzRun(t, fv, seed, 0)
		for crashAt := 1; msg == "" && crashAt <= posts; crashAt++ {
			msg, _ = fuzzRun(t, fv, seed, crashAt)
		}
		if msg != "" {
			t.Errorf("%+v, seed %d:\n%s", fv, seed, msg)
		}
	}
}

type fuzzVariant struct{ noDups, noRefuse bool }

type fuzzState struct {
	exists  bool
	id      string
	url     string
	label   string
	second  string
	code    string
	name    string
	trashed bool
	flagged bool
}

func fuzzRead(h *harness, key string) fuzzState {
	rows := h.fake.byKey("companies", key)
	if len(rows) != 1 {
		return fuzzState{}
	}
	return fuzzRow(rows[0])
}

func fuzzRow(r record) fuzzState {
	s := fuzzState{exists: true}
	s.id, _ = r["id"].(string)
	s.url, _ = fakeGet(r, "domainName.primaryLinkUrl").(string)
	s.label, _ = fakeGet(r, "domainName.primaryLinkLabel").(string)
	s.second = fmt.Sprint(fakeGet(r, "domainName.secondaryLinks"))
	if s.second == "<nil>" {
		s.second = "[]"
	}
	s.code, _ = r["accountCode"].(string)
	s.name, _ = r["name"].(string)
	s.trashed = r["deletedAt"] != nil
	s.flagged = r["externalDeleted"] == true
	return s
}

func fuzzRun(t *testing.T, fv fuzzVariant, seed int64, crashAt int) (string, int) {
	rng := rand.New(rand.NewSource(seed))
	withCodes := rng.Intn(2) == 0
	rejects := rng.Intn(3) == 0 // one document is rejected, under rejected_records skip
	refuses := rng.Intn(5) == 0 // one record's stand-in is refused
	delMode := []string{"", "", `,"delete_mode":"delete"`, `,"delete_mode":"trash"`, flagDeletes}[rng.Intn(5)]
	rich := rng.Intn(2) == 0    // documents may set a link's label and secondary links
	restore := rng.Intn(2) == 0 // restore_deleted
	adopt := rng.Intn(3) == 0 && (delMode == "" || delMode == flagDeletes)
	if adopt {
		rejects = false
	}
	options := delMode
	if rejects {
		options += `,"rejected_records":"skip"`
	}
	if restore {
		options += `,"restore_deleted":true`
	}
	if adopt {
		options += `,"adopt_by":"domainName"`
	}
	tb := companies(options)
	if rich {
		tb = withProjections(tb, proj("domainName/primaryLinkLabel", "/label", "string"), proj("domainName/secondaryLinks", "/links", "array"))
	}
	h := newHarness(t, tb)
	var desc []string
	say := func(f string, a ...any) { desc = append(desc, fmt.Sprintf(f, a...)) }
	say("options %s codes=%v rich=%v", options, withCodes, rich)

	const nKeys = 125
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
	}
	var hot []string
	for _, i := range rng.Perm(nKeys)[:7] {
		hot = append(hot, keys[i])
	}
	slices.Sort(hot)
	pool := []string{"v0.com", "v1.com", "v2.com", "v3.com", "v4.com", "v5.com", "v6.com"}
	codes := []string{"C0", "C1", "C2", "C3", "C4"}

	freeDomains := slices.Clone(pool)
	rng.Shuffle(len(freeDomains), func(i, j int) { freeDomains[i], freeDomains[j] = freeDomains[j], freeDomains[i] })
	freeCodes := slices.Clone(codes)
	rng.Shuffle(len(freeCodes), func(i, j int) { freeCodes[i], freeCodes[j] = freeCodes[j], freeCodes[i] })
	take := func(from *[]string) string {
		if len(*from) == 0 {
			return ""
		}
		v := (*from)[0]
		*from = (*from)[1:]
		return v
	}

	trashedHot := map[string]bool{}
	for _, k := range hot {
		if rng.Intn(5) == 0 {
			say("%s: no record", k)
			continue
		}
		rec := record{"externalId": k, "name": "Old"}
		link := map[string]any{}
		if rng.Intn(6) != 0 {
			if d := take(&freeDomains); d != "" {
				link["primaryLinkUrl"] = d
				if rng.Intn(2) == 0 {
					link["primaryLinkLabel"] = "Label " + k
				}
				if rng.Intn(3) == 0 {
					link["secondaryLinks"] = []any{map[string]any{"url": "https://blog-" + k + ".example", "label": "Blog"}}
				}
			}
		}
		if len(link) > 0 {
			rec["domainName"] = link
		}
		if withCodes && rng.Intn(2) == 0 {
			if c := take(&freeCodes); c != "" {
				rec["accountCode"] = c
			}
		}
		row := h.fake.seed("companies", rec)
		if rng.Intn(8) == 0 {
			h.fake.softDelete("companies", row["id"].(string))
			trashedHot[k] = true
		}
		say("%s: seeded %v trashed=%v", k, rec, trashedHot[k])
	}
	// Records with no document in the transaction.
	var outsiders []string         // ids
	keyless := map[string]string{} // domain -> id of a live record without a key
	for i := 0; i < rng.Intn(3); i++ {
		d := take(&freeDomains)
		if d == "" {
			break
		}
		rec := record{"domainName": map[string]any{"primaryLinkUrl": d}}
		kind := rng.Intn(3)
		if kind != 0 {
			rec["externalId"] = fmt.Sprintf("out%d", i)
		}
		row := h.fake.seed("companies", rec)
		id := row["id"].(string)
		if kind == 2 {
			h.fake.softDelete("companies", id)
		}
		if kind == 0 {
			keyless[d] = id
		}
		outsiders = append(outsiders, id)
		say("outsider %d kind %d holds %s", i, kind, d)
	}

	type want struct {
		set, clear bool
		v          string
		label      string // if set
		links      bool
	}
	wantDomain, wantCode := map[string]want{}, map[string]want{}
	rejected := ""
	if rejects {
		rejected = hot[rng.Intn(len(hot))]
		h.fake.reject = rejectBad
	}
	deleted := ""
	if rng.Intn(3) == 0 {
		deleted = hot[rng.Intn(len(hot))]
		if deleted == rejected {
			rejected = ""
		}
	}
	refusing := false
	old := map[string]fuzzState{}
	for _, k := range hot {
		old[k] = fuzzRead(h, k)
	}
	skipsDoc := func(k string) bool { return trashedHot[k] && !restore }
	adopted := map[string]string{} // outsider id -> key
	wanted := map[string]bool{}
	var items []storeItem
	for _, k := range keys {
		if !slices.Contains(hot, k) {
			items = append(items, h.doc(0, fmt.Sprintf(`{"id":"%s","name":"Filler"}`, k)))
			continue
		}
		if k == deleted {
			say("doc %s: deletion", k)
			items = append(items, h.deletion(0, k))
			continue
		}
		name := "N"
		if k == rejected {
			name = "Bad!"
		}
		doc := fmt.Sprintf(`{"id":"%s","name":"%s"`, k, name)
		switch r := rng.Intn(20); {
		case r < 2:
		case r < 4:
			doc += `,"domain":null`
			wantDomain[k] = want{clear: true}
		default:
			v := pool[rng.Intn(len(pool))]
			for tries := 0; fv.noDups && wanted[v] && tries < 50; tries++ {
				v = pool[rng.Intn(len(pool))]
			}
			if fv.noDups && wanted[v] {
				v = "only-" + strings.ToLower(k) + ".com"
			}
			wanted[v] = true
			doc += `,"domain":"` + v + `"`
			w := want{set: true, v: v}
			if rich && rng.Intn(3) == 0 {
				w.label = "New " + k
				doc += `,"label":"` + w.label + `"`
			}
			if rich && rng.Intn(3) == 0 {
				w.links = true
				doc += `,"links":[{"url":"https://new-` + k + `.example","label":"S"}]`
			}
			wantDomain[k] = w
			if id, ok := keyless[v]; adopt && ok && !old[k].exists && adopted[id] == "" {
				adopted[id] = k
			}
		}
		if withCodes {
			switch r := rng.Intn(20); {
			case r < 6:
			case r < 8:
				doc += `,"code":""`
				wantCode[k] = want{clear: true}
			default:
				v := codes[rng.Intn(len(codes))]
				for tries := 0; fv.noDups && wanted[v] && tries < 50; tries++ {
					v = codes[rng.Intn(len(codes))]
				}
				if fv.noDups && wanted[v] {
					v = "ONLY-" + k
				}
				wanted[v] = true
				doc += `,"code":"` + v + `"`
				wantCode[k] = want{set: true, v: v}
			}
		}
		doc += "}"
		say("doc %s", doc)
		items = append(items, h.doc(0, doc))
	}
	if refuses && !fv.noRefuse {
		// Any write that empties the domain of a record whose document sets one is a stand-in.
		var can []string
		for _, k := range hot {
			if wantDomain[k].set && k != rejected {
				can = append(can, k)
			}
		}
		if len(can) > 0 {
			refused := can[rng.Intn(len(can))]
			say("stand-ins of %s are refused", refused)
			refusing = true
			h.fake.reject = func(_ string, rec record) string {
				u, set := fakeGet(rec, "domainName.primaryLinkUrl").(string)
				if rec["externalId"] == refused && set && (u == "" || strings.HasSuffix(u, ".invalid")) {
					return "Bad"
				}
				if rec["name"] == "Bad!" {
					return "Bad"
				}
				return ""
			}
		}
	}

	oldOut := map[string]fuzzState{}
	for _, id := range outsiders {
		oldOut[id] = fuzzRow(h.fake.row("companies", id))
	}

	posts := 0
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/rest/batch/companies") {
			if posts++; crashAt > 0 && posts >= crashAt {
				h.fake.failures = append(h.fake.failures, fakeFailure{method: http.MethodPost, path: "/rest/batch/companies", status: 401, body: messagesBody("Token invalid.")})
			}
		}
	}
	say("crash at batch %d", crashAt)
	err := h.store(items...)
	total := posts
	h.fake.hook = nil
	h.fake.failures = nil
	if crashAt == 0 && err != nil {
		return strings.Join(desc, "\n") + "\nERROR without crash: " + err.Error(), total
	}
	crashed := err != nil
	if crashed {
		for _, k := range hot {
			say("  after crash %s: %+v", k, fuzzRead(h, k))
		}
		if err := h.store(items...); err != nil {
			return strings.Join(desc, "\n") + "\nERROR on replay: " + err.Error(), total
		}
	}
	skipped := h.stats("companies").skipped

	var bad []string
	holder := func(get func(fuzzState) string, v string) bool {
		for _, r := range h.fake.rows["companies"] {
			if get(fuzzRow(r)) == v {
				return true
			}
		}
		return false
	}
	unapplied := map[string]int{}
	rows := nKeys + len(outsiders) - len(adopted)
	for _, k := range hot {
		got, was := fuzzRead(h, k), old[k]
		if k == deleted {
			exp := was
			switch {
			case !was.exists:
				rows--
			case was.trashed:
			case strings.Contains(delMode, `"delete"`):
				exp = fuzzState{}
				rows--
			case strings.Contains(delMode, "trash"):
				exp.trashed = true
			case strings.Contains(delMode, "flag"):
				exp.flagged = true
			}
			if got != exp {
				bad = append(bad, fmt.Sprintf("%s: deleted, want %+v got %+v (crashed=%v)", k, exp, got, crashed))
			}
			continue
		}
		if k == rejected || skipsDoc(k) {
			if !was.exists {
				rows--
			}
			if got != was {
				bad = append(bad, fmt.Sprintf("%s: rejected or trashed, should be as before: was %+v got %+v (crashed=%v)", k, was, got, crashed))
			}
			continue
		}
		if !got.exists {
			bad = append(bad, k+": no record")
			continue
		}
		if got.trashed {
			bad = append(bad, k+": still trashed")
		}
		// Domain.
		w := wantDomain[k]
		expLabel, expSecond := was.label, was.second
		for id, key := range adopted {
			if key == k {
				expLabel, expSecond = oldOut[id].label, oldOut[id].second
				was = oldOut[id]
			}
		}
		if !was.exists {
			expSecond = "[]"
		}
		if w.label != "" {
			expLabel = w.label
		}
		if w.links {
			expSecond = "[map[label:S url:https://new-" + k + ".example]]"
		}
		switch {
		case w.clear:
			if got.url != "" {
				bad = append(bad, fmt.Sprintf("%s: domain not cleared: %+v", k, got))
			}
		case w.set && got.url == w.v:
			if got.label != expLabel || got.second != expSecond {
				bad = append(bad, fmt.Sprintf("%s: took %s but label/secondary are wrong: was %+v got %+v", k, w.v, was, got))
			}
		default:
			if w.set {
				unapplied["domainName"]++
				if !holder(func(s fuzzState) string { return s.url }, w.v) && wantDomain[rejected].v != w.v && !refusing {
					bad = append(bad, fmt.Sprintf("%s: didn't take %s though no record holds it (crashed=%v)", k, w.v, crashed))
				}
			}
			if got.url != was.url || was.url != "" && (got.label != expLabel || got.second != expSecond) {
				bad = append(bad, fmt.Sprintf("%s: domain should be as before: was %+v got %+v (crashed=%v)", k, was, got, crashed))
			}
		}
		// Code.
		c := wantCode[k]
		switch {
		case c.clear:
			if got.code != "" {
				bad = append(bad, fmt.Sprintf("%s: code not cleared: %+v", k, got))
			}
		case c.set && got.code == c.v:
		default:
			if c.set {
				unapplied["accountCode"]++
				if !holder(func(s fuzzState) string { return s.code }, c.v) && wantCode[rejected].v != c.v && !refusing {
					bad = append(bad, fmt.Sprintf("%s: didn't take code %s though no record holds it (crashed=%v)", k, c.v, crashed))
				}
			}
			if got.code != was.code {
				bad = append(bad, fmt.Sprintf("%s: code should be as before: was %+v got %+v (crashed=%v)", k, was, got, crashed))
			}
		}
	}
	for _, id := range outsiders {
		got := fuzzRow(h.fake.row("companies", id))
		if key := adopted[id]; key != "" {
			if fuzzRead(h, key).id != id {
				bad = append(bad, fmt.Sprintf("outsider %s wasn't adopted by %s", id, key))
			}
			continue
		}
		if got != oldOut[id] {
			bad = append(bad, fmt.Sprintf("outsider %s changed: was %+v got %+v", id, oldOut[id], got))
		}
	}
	for _, f := range []string{"domainName", "accountCode"} {
		extra := 0
		if w := map[string]map[string]want{"domainName": wantDomain, "accountCode": wantCode}[f][rejected]; rejected != "" && w.set {
			extra = 1
		}
		if n := skipped[f+"_held_by_another_record"]; n < unapplied[f] || n > unapplied[f]+extra {
			bad = append(bad, fmt.Sprintf("%s: %d values unapplied but %d skips counted (%v)", f, unapplied[f], n, skipped))
		}
	}
	if n := h.fake.rowCount("companies"); n != rows {
		bad = append(bad, fmt.Sprintf("row count %d, want %d", n, rows))
	}
	// Another replay changes nothing.
	before := map[string]fuzzState{}
	for _, k := range hot {
		before[k] = fuzzRead(h, k)
	}
	if err := h.store(items...); err != nil {
		bad = append(bad, "second replay failed: "+err.Error())
	}
	for _, k := range hot {
		if got := fuzzRead(h, k); got != before[k] && !refusing {
			bad = append(bad, fmt.Sprintf("%s: a second replay changed it: %+v -> %+v", k, before[k], got))
		}
	}
	if len(bad) == 0 {
		return "", total
	}
	for _, k := range hot {
		say("  final %s: %+v", k, fuzzRead(h, k))
	}
	return strings.Join(desc, "\n") + "\nVIOLATIONS:\n  " + strings.Join(bad, "\n  "), total
}
