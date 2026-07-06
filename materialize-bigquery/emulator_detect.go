package main

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	bqv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

// googleBigQueryEndpoint is the Go client's default API base path, used only to
// name the probed server in logs and errors when advanced.endpoint is unset.
const googleBigQueryEndpoint = "https://bigquery.googleapis.com/bigquery/v2/"

// probeRetrySchedule bounds detection-probe retries: len(probeRetrySchedule)+1
// attempts total, sleeping the scheduled durations between them, with each
// attempt capped at probeAttemptTimeout. Worst case the probe fails in about
// two and a half minutes against a server that accepts connections but never
// answers, rather than stalling Validate/Apply/Open indefinitely.
var probeRetrySchedule = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
}

const probeAttemptTimeout = 30 * time.Second

// detectEmulatorGoccy classifies the configured server as either real SaaS
// BigQuery (false) or the goccy/bigquery-emulator - or any other non-Google
// implementation - (true). It runs exactly once per session, at endpoint setup
// (NewEndpoint), and the result is threaded through dialect, client, and
// transactor construction so per-operation code never re-probes. The Spec RPC
// never constructs an endpoint, so producing a spec never requires a live
// server.
func detectEmulatorGoccy(ctx context.Context, cfg config) (bool, error) {
	var opts, err = cfg.clientOptions()
	if err != nil {
		return false, fmt.Errorf("building client options for detection probe: %w", err)
	}

	var endpointDesc = cfg.Advanced.Endpoint
	if endpointDesc == "" {
		endpointDesc = googleBigQueryEndpoint
	}

	isSaaS, err := classifyServerWithRetry(ctx, cfg.effectiveBillingProjectID(), endpointDesc, probeRetrySchedule, opts...)
	if err != nil {
		return false, err
	}

	log.WithFields(log.Fields{
		"endpoint":        endpointDesc,
		"isEmulatorGoccy": !isSaaS,
	}).Info("classified BigQuery server")

	return !isSaaS, nil
}

// classifyServerWithRetry runs the classification probe with retries per
// retrySchedule. A probe error is always inconclusive - never a classification
// - so exhausting retries is a hard error naming the endpoint: silently
// defaulting to either mode would run SaaS SQL against an emulator or
// emulator workarounds against the real service.
func classifyServerWithRetry(ctx context.Context, projectID, endpointDesc string, retrySchedule []time.Duration, opts ...option.ClientOption) (bool, error) {
	svc, err := bqv2.NewService(ctx, opts...)
	if err != nil {
		return false, fmt.Errorf("creating bigquery v2 service for detection probe: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= len(retrySchedule); attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(retrySchedule[attempt-1]):
			}
		}

		isSaaS, err := probeIsSaaSBigQuery(ctx, svc, projectID)
		if err == nil {
			return isSaaS, nil
		}
		lastErr = err

		log.WithFields(log.Fields{
			"endpoint": endpointDesc,
			"attempt":  attempt + 1,
		}).WithError(err).Info("BigQuery server classification probe failed")
	}

	return false, fmt.Errorf("classifying the BigQuery server at %s (%d attempts): %w", endpointDesc, len(retrySchedule)+1, lastErr)
}

// probeIsSaaSBigQuery classifies the connected server as either real SaaS
// BigQuery (true) or the goccy/bigquery-emulator - or any other non-Google
// implementation - (false), by running a trivial `SELECT 1` via the jobs.query
// REST method and inspecting the response body.
//
// Why this probe is reliable:
//
//   - Google's API frontends populate the `kind` discovery field
//     ("bigquery#queryResponse") on every jobs.query response body, per the
//     BigQuery v2 discovery document. goccy/bigquery-emulator marshals its own
//     internal response structs and never sets `kind` (verified hands-on
//     against ghcr.io/goccy/bigquery-emulator: the response carries only
//     jobReference/schema/rows/totalRows/jobComplete).
//   - The signal is in the response BODY, not headers. A proxy configured in
//     front of real SaaS BigQuery via `advanced.endpoint` forwards Google's
//     JSON body verbatim, so proxied SaaS still classifies as SaaS.
//     Header-based signals (e.g. Google's `Server: ESF`) were rejected because
//     proxies routinely rewrite or strip headers.
//   - jobs.query needs no pre-existing dataset or table (the connector may
//     create its dataset later via CreateSchema), costs zero bytes billed, and
//     requires only the bigquery.jobs.create permission the connector already
//     needs to function at all.
//   - The probe is independent of configuration: it does not consult
//     advanced.endpoint or whether credentials are provided; it only inspects
//     the server's response.
//
// A transport-level or API error is returned as an error (inconclusive), never
// mapped to either classification; classifyServerWithRetry layers retry and
// fail-fast handling on top.
func probeIsSaaSBigQuery(ctx context.Context, svc *bqv2.Service, projectID string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, probeAttemptTimeout)
	defer cancel()

	// UseLegacySql must be sent explicitly: the server defaults an omitted
	// field to legacy SQL.
	var useLegacySQL = false
	resp, err := svc.Jobs.Query(projectID, &bqv2.QueryRequest{
		Query:        "SELECT 1",
		UseLegacySql: &useLegacySQL,
	}).Context(ctx).Do()
	if err != nil {
		return false, fmt.Errorf("running detection probe query: %w", err)
	}

	return resp.Kind == "bigquery#queryResponse", nil
}
