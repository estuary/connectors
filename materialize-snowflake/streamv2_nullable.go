package connector

import (
	"context"
	"fmt"
	"strings"
	"text/template"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	log "github.com/sirupsen/logrus"
)

// renderDropNotNulls renders the statement that drops the NOT NULL constraint
// from every existing column that has one, or an empty string if none does.
func renderDropNotNulls(tpl *template.Template, table sql.Table, existing *boilerplate.ExistingResource) (string, error) {
	var notNull []boilerplate.ExistingField
	for _, field := range existing.AllFields() {
		if !field.Nullable {
			notNull = append(notNull, field)
		}
	}
	if len(notNull) == 0 {
		return "", nil
	}

	var w strings.Builder
	if err := tpl.Execute(&w, sql.TableAlter{Table: table, DropNotNulls: notNull}); err != nil {
		return "", fmt.Errorf("rendering alter table columns statement: %w", err)
	}
	return w.String(), nil
}

// dropStreamV2NotNulls drops every NOT NULL constraint from the tables of the
// snowpipe_streaming_v2 bindings. Snowflake's streaming pipe goes on enforcing
// a NOT NULL constraint for about two minutes after the constraint is dropped,
// and it rejects and discards every row that holds a null in that column
// meanwhile, so the constraints are dropped before any field can become
// nullable.
func (d *transactor) dropStreamV2NotNulls(ctx context.Context, is *boilerplate.InfoSchema) error {
	for _, b := range d.bindings {
		if !b.streamingV2 {
			continue
		}
		var existing = is.GetResource(b.target.Path)
		if existing == nil {
			continue
		}

		stmt, err := renderDropNotNulls(d.templates.alterTableColumns, b.target, existing)
		if err != nil {
			return err
		} else if stmt == "" {
			continue
		}

		if _, err := d.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("dropping NOT NULL constraints on %s: %w", b.target.Identifier, err)
		}
		log.WithField("table", b.target.Identifier).Info("dropped the NOT NULL constraints of a snowpipe_streaming_v2 table")
	}
	return nil
}
