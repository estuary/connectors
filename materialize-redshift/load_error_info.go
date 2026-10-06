package connector

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/jackc/pgx/v5"
)

// likePatternReplacer escapes a SQL LIKE pattern that is using ^ as the escape
// character.  This char is picked since it is not special in SQL.
var likePatternReplacer = strings.NewReplacer(
	`^`, `^^`,
	`_`, `^_`,
	`%`, `^%`,
)

type loadErrorInfo struct {
	errMsg    string
	errCode   int
	colName   string
	colType   string
	colLength string // Yes, this is actually a char(10) column in Redshift for some reason
}

// getLoadErrorInfo looks up the load error recorded for any of the staged
// files, which may sit under several prefixes when shards' files are copied
// together.
func getLoadErrorInfo(ctx context.Context, conn *pgx.Conn, dialect sql.Dialect, bucket string, files []string) (loadErrorInfo, error) {
	var prefixes []string
	for _, f := range files {
		bucketPath := fmt.Sprintf("s3://%s/%s/", bucket, path.Dir(f))
		bucketPattern := likePatternReplacer.Replace(bucketPath) + "%"
		if p := fmt.Sprintf("file_name LIKE %s ESCAPE '^'", dialect.Literal(bucketPattern)); !slices.Contains(prefixes, p) {
			prefixes = append(prefixes, p)
		}
	}
	if len(prefixes) == 0 {
		return loadErrorInfo{}, fmt.Errorf("no staged files to look up")
	}

	q := fmt.Sprintf(`
	SELECT 
		error_message,
		error_code, 
		column_name,
		column_type,
		column_length
	FROM sys_load_error_detail 
	WHERE %s
	LIMIT 1;
	`,
		strings.Join(prefixes, " OR "),
	)

	var out loadErrorInfo
	if err := conn.QueryRow(ctx, q).Scan(&out.errMsg, &out.errCode, &out.colName, &out.colType, &out.colLength); err != nil {
		return loadErrorInfo{}, err
	}

	// Trim excess whitespace from the CHAR columns, since they will be padded with extra spaces out
	// to their CHAR(X) types from Redshift.
	out.errMsg = strings.TrimSpace(out.errMsg)
	out.colName = strings.TrimSpace(out.colName)
	out.colType = strings.TrimSpace(out.colType)
	out.colLength = strings.TrimSpace(out.colLength)

	return out, nil
}
