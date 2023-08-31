package main

import (
	"context"
	"strings"
	"strconv"
	"fmt"
	"sort"

	stdsql "database/sql"
	log "github.com/sirupsen/logrus"
)

const collationQuery = "SELECT name FROM sys.fn_helpcollations() WHERE name LIKE 'Latin1_General_%_BIN%'"

// The logic for selecting a collation is as follows:
// 1. We prefer BIN2 to BIN if available. Although BIN2 and BIN are the same
// when it comes to comparing values (which is what we are interested in
// directly), BIN2 is more accurate when it comes to sorting values. We choose
// BIN2 for two reasons: it is likely to be the best-performing collation [0], and
// also the user that is using the database will have more accurate
// sorting results.
//
// 2. We prefer UTF8 to non-UTF8 encodings, as this allows the customer to have
// a wider range of unicode characters stored
//
// 3. We prefer newer versions of collations to older versions (e.g. 140 is
// better than 100 than 90 and so on...) as newer versions have more accurate
// code point mappings.
// 
// [0]: https://learn.microsoft.com/en-us/sql/relational-databases/collations/collation-and-unicode-support?view=sql-server-ver16#Supplementary_Characters
func getCollation(ctx context.Context, cfg *config) (string, error) {
	db, err := stdsql.Open("sqlserver", cfg.ToURI());
	if err != nil {
		return "", fmt.Errorf("opening database: %w", err)
	}
	conn, err := db.Conn(ctx);
	if err != nil {
		return "", fmt.Errorf("connecting to sqlserver: %w", err)
	}

	rows, err := conn.QueryContext(ctx, collationQuery);
	if err != nil {
		return "", fmt.Errorf("querying available collations: %w", err)
	}

	var collations []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return "", fmt.Errorf("reading collation: %w", err)
		}

		collations = append(collations, c)
	}

	sortCollations(collations)

	log.WithField("collations", collations).Debug("sorted collations based on preference, the first will be used for primary keys")

	return collations[0], nil
}

func sortCollations(collations []string) {
	// We score collations based on the criteria explained in the large comment
	// above this function. Negative score means a is smaller in sorting order
	// (i.e. we prefer it to b).
	sort.Slice(collations, func(i, j int) bool {
		var a = collations[i]
		var b = collations[j]
		var aBin2 = strings.Contains(a, "BIN2")
		var bBin2 = strings.Contains(b, "BIN2")
		var bin2Score = 0
		if aBin2 && !bBin2 {
			bin2Score = -1
		} else if !aBin2 && bBin2 {
			bin2Score = 1
		}

		var aUTF8 = strings.Contains(a, "UTF8")
		var bUTF8 = strings.Contains(b, "UTF8")
		var UTF8Score = 0
		if aUTF8 && !bUTF8 {
			UTF8Score = -1
		} else if !aUTF8 && bUTF8 {
			UTF8Score = 1
		}

		// Latin1_General_{VERSION}
		var aVersion, _ = strconv.Atoi(strings.Split(a, "_")[2])
		var bVersion, _ = strconv.Atoi(strings.Split(b, "_")[2])

		var versionScore = 0
		if aVersion > bVersion {
			versionScore = -1
		} else if bVersion > aVersion {
			versionScore = 1
		}

		var finalScore = bin2Score + UTF8Score + versionScore

		return finalScore < 0
	})
}

