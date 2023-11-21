package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
)

type loadErrorInfo struct {
	errMsg    string
	errCode   int
	colName   string
	colType   string
	colLength string // Yes, this is actually a char(10) column in Redshift for some reason
}

func getLoadErrorInfo(ctx context.Context, conn *pgx.Conn, bucket, prefix string) (loadErrorInfo, error) {
	q := fmt.Sprintf(`
	SELECT 
		error_message,
		error_code, 
		column_name,
		column_type,
		column_length
	FROM sys_load_error_detail 
	WHERE file_name LIKE 's3://%s/%s/%%';
	`,
		bucket,
		prefix,
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
