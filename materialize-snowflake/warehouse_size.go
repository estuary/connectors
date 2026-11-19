package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	log "github.com/sirupsen/logrus"
)

// warehouseSize is a value of the WAREHOUSE_SIZE property of a warehouse.
type warehouseSize string

// warehouseSizes are the sizes that ALTER WAREHOUSE accepts unquoted.
var warehouseSizes = []warehouseSize{
	"XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE", "X4LARGE", "X5LARGE", "X6LARGE",
}

// backfillingStateKey is the checkpoint key of the item that holds
// checkpointItem.Backfilling. Its unencoded slash keeps it from being the
// state key of a binding.
const backfillingStateKey = "/backfilling"

// currentWarehouse returns the quoted identifier of the session's warehouse,
// which cfg.Warehouse may not be: it can be empty, and Snowflake resolved it
// without regard to case.
func currentWarehouse(ctx context.Context, db *stdsql.DB) (string, error) {
	var name *string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_WAREHOUSE();").Scan(&name); err != nil {
		return "", fmt.Errorf("querying the current warehouse: %w", err)
	} else if name == nil {
		return "", fmt.Errorf("warehouse sizes are configured but the session has no warehouse")
	}
	return `"` + strings.ReplaceAll(*name, `"`, `""`) + `"`, nil
}

// usesWarehouse reports whether the warehouse runs the binding's stores, which
// it does not for Snowpipe and Snowpipe Streaming.
func (b *binding) usesWarehouse() bool {
	return !b.streaming && !b.streamingV2 && b.pipeName == ""
}

// anyBackfilling reports whether a binding of the task is being backfilled.
func (d *transactor) anyBackfilling() bool {
	for _, b := range d.bindings {
		if d.backfilling[b.target.StateKey] {
			return true
		}
	}
	return false
}

// setWarehouseSize does nothing if size is the size last set in this session.
func (d *transactor) setWarehouseSize(ctx context.Context, size warehouseSize) error {
	if size == d.warehouseSize {
		return nil
	}

	var query = fmt.Sprintf("ALTER WAREHOUSE %s SET WAREHOUSE_SIZE = %s;", d.warehouse, size)
	if _, err := d.db.ExecContext(ctx, query); err != nil {
		return cerrors.NewUserError(
			fmt.Errorf("resizing warehouse %s to %s: %w", d.warehouse, size, err),
			fmt.Sprintf("Unable to set the size of warehouse %s to %s: %s", d.warehouse, size, err),
		)
	}

	log.WithFields(log.Fields{"warehouse": d.warehouse, "size": size}).Info("resized the warehouse")
	d.warehouseSize = size
	return nil
}
