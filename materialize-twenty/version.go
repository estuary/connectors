package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// minDeleteVersion is the oldest Twenty that applies the connector's deletes. From v2.30 to v2.34
// a DELETE whose filter holds deletedAt[is]:NULL matches nothing and answers no error, and before
// v2.30 the metadata API answers in another shape (local probes of each version, Nov 2026).
var minDeleteVersion = [3]int{2, 35, 0}

// version is a major, minor and patch version, then 1 for a release or 0 for a pre-release like
// v2.35.0-canary, which comes before v2.35.0.
type version [4]int

// atLeast says whether v is min or later.
func (v version) atLeast(min [3]int) bool {
	return slices.Compare(v[:], []int{min[0], min[1], min[2], 1}) >= 0
}

// appVersion returns the version Twenty reports at /client-config, which takes no key, or "" if
// Twenty doesn't say. It doesn't go through request, so that a proxy in front of the page can't
// fail the connector.
func (c *client) appVersion(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/client-config", nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.http.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	var out struct {
		AppVersion string `json:"appVersion"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return ""
	}
	return out.AppVersion
}

// parseVersion reads a version like "v2.52.0" or "v2.35.0-canary".
func parseVersion(s string) (version, bool) {
	var v version
	s = strings.TrimPrefix(strings.TrimSpace(s), "v")
	s, _, _ = strings.Cut(s, "+")
	s, pre, _ := strings.Cut(s, "-")
	if pre == "" {
		v[3] = 1
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return v, false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return v, false
		}
		v[i] = n
	}
	return v, true
}

// checkDeleteVersion refuses the collections that delete records in Twenty when Twenty is too old
// to apply the deletes. Bindings that don't delete send no request. Twenty Cloud runs the latest
// version, and a Twenty that doesn't say its version is taken to be recent enough.
func checkDeleteVersion(ctx context.Context, c *client, deleting []string) error {
	if len(deleting) == 0 || c.baseURL == defaultBaseURL {
		return nil
	}
	raw := c.appVersion(ctx)
	v, ok := parseVersion(raw)
	if !ok {
		log.WithField("version", raw).Info("Twenty didn't say which version it runs, so the connector can't check that it applies deletes")
		return nil
	}
	if v.atLeast(minDeleteVersion) {
		return nil
	}
	err := fmt.Errorf("Twenty %s skips the deletes the connector sends, without an error, before v2.35. "+
		"Upgrade Twenty, or set delete_mode to %q or %q for %s", raw, deleteFlag, deleteIgnore, strings.Join(deleting, ", "))
	return userError(err)
}

// specResources parses the resource config of each of a materialization's bindings.
func specResources(spec *pf.MaterializationSpec) ([]resource, error) {
	out := make([]resource, len(spec.Bindings))
	for i, b := range spec.Bindings {
		if err := boilerplate.UnmarshalStrict(b.ResourceConfigJson, &out[i]); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
	}
	return out, nil
}

// deletingCollections lists the collections of a materialization whose bindings delete records.
func deletingCollections(spec *pf.MaterializationSpec, resources []resource) []string {
	var out []string
	for i, res := range resources {
		if res.deletes() {
			out = append(out, spec.Bindings[i].Collection.Name.String())
		}
	}
	return out
}

// deletes says whether a binding deletes records in Twenty.
func (r resource) deletes() bool {
	return r.deleteMode() == deleteTrash || r.deleteMode() == deleteDelete
}
