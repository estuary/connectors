package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/estuary/connectors/go-types/airbyte"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

func main() {
	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

var spec = airbyte.Spec{
	SupportsIncremental:           true,                            // TODO(wgd): Verify that this is true once implemented
	SupportedDestinationSyncModes: airbyte.AllDestinationSyncModes, // TODO(wgd): Verify that this is true once implemented
	ConnectionSpecification:       json.RawMessage(configSchema),
}

type Config struct {
	ConnectionURI string `json:"connectionURI"`
}

func (c *Config) Validate() error { return nil }

const configSchema = `{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title":   "Postgres Source Spec",
	"type":    "object",
	"properties": {
		"connectionURI": {
			"type":        "string",
			"title":       "Database Connection URI",
			"description": "Connection parameters, as a libpq-compatible connection string",
			"default":     "postgres://flow:flow@localhost:5432/flow"
		}
	},
	"required": [ "connectionURI" ]
}`

func doCheck(args airbyte.CheckCmd) error {
	result := &airbyte.ConnectionStatus{Status: airbyte.StatusSucceeded}
	if _, err := discoverCatalog(args.ConfigFile); err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}
	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:             airbyte.MessageTypeConnectionStatus,
		ConnectionStatus: result,
	})
}

func doDiscover(args airbyte.DiscoverCmd) error {
	catalog, err := discoverCatalog(args.ConfigFile)
	if err != nil {
		return err
	}
	log.Printf("Discover completed with %d streams", len(catalog.Streams))
	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:    airbyte.MessageTypeCatalog,
		Catalog: catalog,
	})
}

type DBTable struct {
	TableSchema string
	TableName   string
	Columns     []DBColumn
	PrimaryKeys []DBPrimaryKey
}

func discoverCatalog(configFile airbyte.ConfigFile) (*airbyte.Catalog, error) {
	var config Config
	if err := configFile.Parse(&config); err != nil {
		return nil, err
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, config.ConnectionURI)
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect to database")
	}
	defer conn.Close(ctx)

	// Get lists of all columns and primary keys in the database
	columns, err := getColumns(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list database columns")
	}
	primaryKeys, err := getPrimaryKeys(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list database primary keys")
	}

	// Aggregate column and primary key information into DBTable structs using
	// (TableSchema, TableName) tuples combined into a string ID.
	tables := make(map[string]*DBTable)
	for _, column := range columns {
		id := column.TableSchema + ":" + column.TableName
		if _, ok := tables[id]; !ok {
			tables[id] = &DBTable{TableSchema: column.TableSchema, TableName: column.TableName}
		}
		tables[id].Columns = append(tables[id].Columns, column)
	}
	for _, key := range primaryKeys {
		id := key.TableSchema + ":" + key.TableName
		if _, ok := tables[id]; !ok {
			// We only need primary key info for tables which getColumns() returned,
			// so if the table doesn't exist at this point then skip it.
			continue
		}
		tables[id].PrimaryKeys = append(tables[id].PrimaryKeys, key)
	}

	catalog := new(airbyte.Catalog)
	for _, table := range tables {
		log.Printf("Found table (%v, %v)", table.TableSchema, table.TableName)
		log.Printf("  Columns: %v", table.Columns)
		log.Printf("  Primary Keys: %v", table.PrimaryKeys)

		// TODO: Maybe generate the schema in a less hackish fashion
		rowSchema := `{"type":"object","properties":{`
		for idx, column := range table.Columns {
			if idx > 0 {
				rowSchema += ","
			}
			rowSchema += fmt.Sprintf("%q:%s", column.ColumnName, postgresTypeToJSON[column.DataType])
		}
		rowSchema += `}}`
		log.Printf("  Schema: %v", rowSchema)

		var primaryKeys []string
		for _, pk := range table.PrimaryKeys {
			primaryKeys = append(primaryKeys, pk.ColumnName)
		}

		catalog.Streams = append(catalog.Streams, airbyte.Stream{
			Name:                    table.TableName,
			Namespace:               table.TableSchema,
			JSONSchema:              json.RawMessage(rowSchema),
			SupportedSyncModes:      airbyte.AllSyncModes,
			SourceDefinedCursor:     true,
			SourceDefinedPrimaryKey: [][]string{primaryKeys},
		})
	}
	return catalog, err
}

// Airbyte's Postgres Source Discovery:
// {"type":"CATALOG",
//  "catalog":{
// 	 "streams":[
// 		 {"name":"surnames",
// 		  "json_schema":{"type":"object","properties":{"name":{"type":"string"},"id":{"type":"number"},"_ab_cdc_lsn":{"type":"number"},"_ab_cdc_updated_at":{"type":"string"},"_ab_cdc_deleted_at":{"type":"string"}}},
// 		  "supported_sync_modes":["full_refresh","incremental"],
// 		  "source_defined_cursor":true,
// 		  "default_cursor_field":[],
// 		  "source_defined_primary_key":[["id"]],
// 		  "namespace":"public"},
// 		 {"name":"babynames",
// 		  "json_schema":{"type":"object","properties":{"name":{"type":"string"},"id":{"type":"number"},"_ab_cdc_lsn":{"type":"number"},"_ab_cdc_updated_at":{"type":"string"},"_ab_cdc_deleted_at":{"type":"string"}}},
// 		  "supported_sync_modes":["full_refresh","incremental"],
// 		  "source_defined_cursor":true,
// 		  "default_cursor_field":[],
// 		  "source_defined_primary_key":[["id"]],
// 		  "namespace":"public"}]}}

// Our Postgres Discovery:
// {"type":"CATALOG",
//  "catalog":{
//    "streams":[
//      {"name":"babynames",
//       "json_schema":{"type":"object","properties":{"id":{"type":"number"},"name":{"type":"string"}}},
//       "supported_sync_modes":["incremental","full_refresh"],
//       "source_defined_cursor":true,
//       "source_defined_primary_key":[["id"]],
//       "namespace":"public"},
//      {"name":"surnames",
//       "json_schema":{"type":"object","properties":{"id":{"type":"number"},"name":{"type":"string"}}},
//       "supported_sync_modes":["incremental","full_refresh"],
//       "source_defined_cursor":true,
//       "source_defined_primary_key":[["id"]],
//       "namespace":"public"}]}}

// So pretty similar, except that the Airbyte source adds some metadata fields
// and also has `default_cursor_field`.

type DBColumn struct {
	TableSchema string
	TableName   string
	ColumnName  string
	ColumnIndex int
	IsNullable  bool
	DataType    string
}

const COLUMNS_QUERY = `
  SELECT table_schema, table_name, ordinal_position, column_name, is_nullable::boolean, udt_name
  FROM information_schema.columns
  WHERE table_schema != 'pg_catalog' AND table_schema != 'information_schema'
        AND table_schema != 'pg_internal' AND table_schema != 'catalog_history'
  ORDER BY table_schema, table_name, ordinal_position;`

func getColumns(ctx context.Context, conn *pgx.Conn) ([]DBColumn, error) {
	var columns []DBColumn
	var sc DBColumn
	_, err := conn.QueryFunc(ctx, COLUMNS_QUERY, nil,
		[]interface{}{&sc.TableSchema, &sc.TableName, &sc.ColumnIndex, &sc.ColumnName, &sc.IsNullable, &sc.DataType},
		func(r pgx.QueryFuncRow) error {
			columns = append(columns, sc)
			return nil
		})
	return columns, err
}

var postgresTypeToJSON = map[string]string{
	"int4":    `{"type":"number"}`,
	"varchar": `{"type":"string"}`,
}

type DBPrimaryKey struct {
	TableSchema string
	TableName   string
	ColumnName  string
	ColumnIndex int
	PKName      string
}

// Query copied from pgjdbc's method PgDatabaseMetaData.getPrimaryKeys() with
// the always-NULL `TABLE_CAT` column omitted.
const PRIMARY_KEYS_QUERY = `
  SELECT result.TABLE_SCHEM, result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ, result.PK_NAME
  FROM (
    SELECT n.nspname AS TABLE_SCHEM,
      ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,
      (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME,
      information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM
    FROM pg_catalog.pg_class ct
      JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
      JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
      JOIN pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
      JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
    WHERE true
     AND i.indisprimary
  ) result
  WHERE result.A_ATTNUM = (result.KEYS).x
  ORDER BY result.table_name, result.pk_name, result.key_seq;
`

func getPrimaryKeys(ctx context.Context, conn *pgx.Conn) ([]DBPrimaryKey, error) {
	var keys []DBPrimaryKey
	var sk DBPrimaryKey
	_, err := conn.QueryFunc(ctx, PRIMARY_KEYS_QUERY, nil,
		[]interface{}{&sk.TableSchema, &sk.TableName, &sk.ColumnName, &sk.ColumnIndex, &sk.PKName},
		func(r pgx.QueryFuncRow) error {
			keys = append(keys, sk)
			return nil
		})
	return keys, err
}

const SLOT_NAME = "estuary_flow_slot"
const OUTPUT_PLUGIN = "pgoutput"

func doRead(args airbyte.ReadCmd) error {
	var config Config
	if err := args.ConfigFile.Parse(&config); err != nil {
		return err
	}
	var catalog airbyte.ConfiguredCatalog
	if err := args.CatalogFile.Parse(&catalog); err != nil {
		return errors.Wrap(err, "unable to parse catalog")
	}

	ctx := context.Background()
	connConfig, err := pgx.ParseConfig(config.ConnectionURI)
	if err != nil {
		return err
	}
	// The URI provided in config.json should lack the `replication=database`
	// setting in order for discovery to work using the same URI, but that means
	// that we have to explicitly add `replication=database` for reads.
	connConfig.RuntimeParams["replication"] = "database"
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return errors.Wrap(err, "unable to connect to database")
	}
	defer conn.Close(ctx)
	pgConn := conn.PgConn()

	sysident, err := pglogrepl.IdentifySystem(ctx, pgConn)
	if err != nil {
		return errors.Wrap(err, "unable to identify system")
	}
	log.Printf("IdentifySystem: %v", sysident)

	_, err = pglogrepl.CreateReplicationSlot(ctx, pgConn, SLOT_NAME, OUTPUT_PLUGIN, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		return errors.Wrap(err, "unable to create replication slot")
	}

	// TODO: Use createSlotResult.SnapshotName to perform a consistent read of all data prior
	// to actually starting replication here

	startLSN := sysident.XLogPos
	if err = pglogrepl.StartReplication(ctx, pgConn, SLOT_NAME, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{`"proto_version" '1'`, `"publication_names" 'estuary_flow_publication'`},
	}); err != nil {
		return errors.Wrap(err, "unable to start replication")
	}

	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		// TODO: Suppose that we use this same standby message deadline as the
		// trigger for emitting a new state message to stdout? Probably using a
		// dirty flag to avoid redundancy, but maybe not even bothering with that
		// since one message every 10s in the absence of any changes is negligible.
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), pgConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		msg, err := pgConn.ReceiveMessage(receiveCtx)
		cancel()
		if pgconn.Timeout(err) {
			continue
		}
		if err != nil {
			return errors.Wrap(err, "error receiving message")
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return errors.Wrap(err, "error parsing keepalive")
				}
				log.Printf("KeepAlive: ServerWALEnd=%q, ReplyRequested=%v", pkm.ServerWALEnd, pkm.ReplyRequested)
				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Now()
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return errors.Wrap(err, "error parsing XLogData")
				}
				if err := handleChangeMessage(ctx, xld.WALStart, xld.ServerWALEnd, xld.ServerTime, xld.WALData); err != nil {
					return err
				}
				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			default:
				log.Printf("Received unknown CopyData message: %v", msg)
			}
		default:
			log.Printf("Received unexpected message: %v", msg)
		}
	}

	// TODO: Implement
	return nil
}

type RelationID = uint32

var relations = make(map[RelationID]pglogrepl.RelationMessage)

func handleChangeMessage(ctx context.Context, walStart, serverWALEnd pglogrepl.LSN, serverTime time.Time, walData []byte) error {
	msg, err := pglogrepl.Parse(walData)
	if err != nil {
		return errors.Wrap(err, "error parsing logical replication message")
	}

	log.Printf("XLogData(Type=%s, WALStart=%q, ServerWALEnd=%q)", msg.Type(), walStart, serverWALEnd)

	switch msg := msg.(type) {
	case *pglogrepl.BeginMessage:
		log.Printf("Begin(finalLSN=%q, xid=%v)", msg.FinalLSN, msg.Xid)
	case *pglogrepl.CommitMessage:
		log.Printf("Commit(commitLSN=%q, txEndLSN=%q)", msg.CommitLSN, msg.TransactionEndLSN)
	// case *pglogrepl.OriginMessage:
	// 	log.Printf("  Origin()")
	// case *pglogrepl.TypeMessage:
	// 	log.Printf("  Type()")
	// case *pglogrepl.TruncateMessage:
	// 	log.Printf("  Truncate()")
	case *pglogrepl.RelationMessage:
		// Keep track of the relation in order to understand future Insert/Update/Delete messages
		//
		// TODO(wgd): How do we know when to delete a relation? Worst-case we can use a timestamp
		// and a separate grooming thread to delete them after some time window has elapsed, but
		// I haven't been able to find any documentation of any of these logical replication
		// messages so I'm hesitant to make assumptions about anything.
		relations[msg.RelationID] = *msg
	case *pglogrepl.InsertMessage:
		return handleDataMessage(ctx, msg.Type(), msg.RelationID, msg.Tuple)
	case *pglogrepl.UpdateMessage:
		return handleDataMessage(ctx, msg.Type(), msg.RelationID, msg.NewTuple)
	case *pglogrepl.DeleteMessage:
		return handleDataMessage(ctx, msg.Type(), msg.RelationID, msg.OldTuple)
	default:
		log.Printf("Unhandled message type %q", msg.Type())
	}
	return nil
}

func handleDataMessage(ctx context.Context, msgType pglogrepl.MessageType, relID RelationID, tuple *pglogrepl.TupleData) error {
	rel, ok := relations[relID]
	if !ok {
		return errors.Errorf("unknown relation %d", relID)
	}

	fields := make(map[string]interface{})
	if msgType == pglogrepl.MessageTypeDelete {
		fields["_deleted"] = true
	}

	log.Printf("%s(rid=%v, namespace=%v, relName=%v)", msgType, rel.RelationID, rel.Namespace, rel.RelationName)
	if tuple != nil {
		for idx, col := range tuple.Columns {
			log.Printf("  (name=%q, type=%q, data=%q)", rel.Columns[idx].Name, col.DataType, col.Data)

			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n':
				fields[colName] = nil
			case 't':
				fields[colName] = encodeColumnData(col.Data, rel.Columns[idx].DataType, rel.Columns[idx].TypeModifier)
			default:
				return errors.Errorf("unhandled column data type %v", col.DataType)
			}
		}
	}

	rawData, err := json.Marshal(fields)
	if err != nil {
		return errors.Wrap(err, "error encoding message data")
	}
	if err := json.NewEncoder(os.Stdout).Encode(airbyte.Message{
		Type: airbyte.MessageTypeRecord,
		Record: &airbyte.Record{
			Stream:    rel.RelationName,
			Namespace: rel.Namespace,
			EmittedAt: time.Now().Unix(),
			Data:      json.RawMessage(rawData),
		},
	}); err != nil {
		return errors.Wrap(err, "error writing output message")
	}
	return nil
}

func encodeColumnData(data []byte, dataType, typeMod uint32) interface{} {
	// TODO(wgd): Use `dataType` and `typeMod` to more intelligently convert
	// text-format values from Postgres into JSON-encodable values.
	return string(data)
}
