package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	_ "github.com/go-mysql-org/go-mysql/driver"

	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbName = flag.String("db_name", "mysql", "Use the named database for tests")

	dbCaptureAddress = flag.String("db_capture_address", "localhost:3306", "The database server address to use for test captures")
	dbCaptureUser    = flag.String("db_capture_user", "flow_capture", "The user to perform captures as")
	dbCapturePass    = flag.String("db_capture_pass", "secret1234", "The password for the capture user")

	dbControlAddress = flag.String("db_control_address", "localhost:3306", "The database server address to use for test setup/control operations. Leave unset to use capture settings.")
	dbControlUser    = flag.String("db_control_user", "root", "The user for test setup/control operations. Leave unset to use capture settings.")
	dbControlPass    = flag.String("db_control_pass", "secret1234", "The password the the test setup/control user. Leave unset to use capture settings.")

	useMyISAM                = flag.Bool("use_myisam_engine", false, "When set, all test tables will be created using the MyISAM storage engine")
	skipBinlogRetentionCheck = flag.Bool("skip_binlog_retention_check", false, "When set, skips the binlog retention sanity check")

	testFeatureFlags = flag.String("feature_flags", "", "Feature flags to apply to all test captures.")
)

const testSchemaName = "test"

func TestMain(m *testing.M) {
	flag.Parse()

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		level, err := logrus.ParseLevel(logLevel)
		if err != nil {
			logrus.WithField("level", logLevel).Fatal("invalid log level")
		}
		logrus.SetLevel(level)
	} else {
		logrus.SetLevel(logrus.DebugLevel)
	}

	// Set a 900MiB memory limit, same as we use in production.
	debug.SetMemoryLimit(900 * 1024 * 1024)

	os.Exit(m.Run())
}

func mysqlTestBackend(t testing.TB) *testBackend {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return nil
	}

	// During tests, establish a fence every 3 seconds during indefinite streaming.
	sqlcapture.StreamingFenceInterval = 3 * time.Second

	// During tests, the stream-to-fence watchdog timeout should be much lower.
	streamToFenceWatchdogTimeout = 2 * time.Second

	logrus.WithFields(logrus.Fields{
		"user": *dbControlUser,
		"addr": *dbControlAddress,
	}).Info("opening control connection")
	var conn, err = client.Connect(*dbControlAddress, *dbControlUser, *dbControlPass, *dbName)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	if *useMyISAM { // Allow manual testing against MyISAM tables
		conn.Execute("SET default_storage_engine = MyISAM;")
	}

	// Construct the capture config
	var captureConfig = Config{
		Address:  *dbCaptureAddress,
		User:     *dbCaptureUser,
		Password: *dbCapturePass,
		Advanced: advancedConfig{
			DBName:                   *dbName,
			SkipBinlogRetentionCheck: *skipBinlogRetentionCheck,
		},
	}
	captureConfig.Advanced.FeatureFlags = *testFeatureFlags
	captureConfig.Advanced.BackfillChunkSize = 16
	if err := captureConfig.Validate(); err != nil {
		t.Fatalf("error validating capture config: %v", err)
	}
	captureConfig.SetDefaults(t.Name())

	return &testBackend{control: conn, config: captureConfig}
}

type testBackend struct {
	control *client.Conn
	config  Config
}

func (tb *testBackend) UpperCaseMode() bool { return false }

func (tb *testBackend) lowerTuningParameters(t testing.TB) {
	var prevBufferSize = replicationBufferSize
	t.Cleanup(func() { replicationBufferSize = prevBufferSize })
	replicationBufferSize = 0
}

func (tb *testBackend) CaptureSpec(ctx context.Context, t testing.TB, streamMatchers ...*regexp.Regexp) *st.CaptureSpec {
	var sanitizers = make(map[string]*regexp.Regexp)
	sanitizers[`"<TIMESTAMP>"`] = regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]+:[0-9]+)"`)
	sanitizers[`"cursor":"binlog.000123:56789:123"`] = regexp.MustCompile(`"cursor":"[^"]+\.[0-9]+:[0-9]+:[0-9]+"`)
	sanitizers[`"cursor":"binlog.000123:56789"`] = regexp.MustCompile(`"cursor":"[^"]+\.[0-9]+:[0-9]+"`)
	sanitizers[`"ts_ms":1111111111111`] = regexp.MustCompile(`"ts_ms":[0-9]+`)
	sanitizers[`"txid":"11111111-1111-1111-1111-111111111111:111"`] = regexp.MustCompile(`"txid":"[0-9a-f-]+:[0-9]+"`)

	var cfg = tb.config
	var cs = &st.CaptureSpec{
		Driver:       mysqlDriver,
		EndpointSpec: &cfg,
		Validator:    &st.OrderedCaptureValidator{IncludeSourcedSchemas: true},
		Sanitizers:   sanitizers,
	}
	if len(streamMatchers) > 0 {
		cs.Bindings = tests.DiscoverBindings(ctx, t, tb, streamMatchers...)
	}
	return cs
}

func (tb *testBackend) CreateTable(ctx context.Context, t testing.TB, suffix string, tableDef string) string {
	t.Helper()

	var tableName = testSchemaName + "." + strings.TrimPrefix(t.Name(), "Test")
	if suffix != "" {
		tableName += "_" + suffix
	}
	for _, str := range []string{"/", "=", "(", ")"} {
		tableName = strings.ReplaceAll(tableName, str, "_")
	}

	logrus.WithFields(logrus.Fields{"table": tableName, "cols": tableDef}).Debug("creating test table")
	tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %s%s;`, tableName, tableDef))
	t.Cleanup(func() {
		logrus.WithField("table", tableName).Debug("destroying test table")
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE %s;`, tableName))
	})
	return tableName
}

func (tb *testBackend) Insert(ctx context.Context, t testing.TB, table string, rows [][]interface{}) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	if err := tb.control.Begin(); err != nil {
		t.Fatalf("error beginning transaction: %v", err)
	}
	var argc = len(rows[0])
	var query = fmt.Sprintf("INSERT IGNORE INTO %s VALUES %s", table, argsTuple(argc))
	for _, row := range rows {
		if len(row) != argc {
			t.Fatalf("incorrect number of values in row %q (expected %d)", row, len(rows[0]))
		}
		tb.Query(ctx, t, query, row...)
	}
	if err := tb.control.Commit(); err != nil {
		t.Fatalf("error committing transaction: %v", err)
	}
}

func argsTuple(argc int) string {
	var tuple = "(?"
	for idx := 1; idx < argc; idx++ {
		tuple += ",?"
	}
	return tuple + ")"
}

func (tb *testBackend) Update(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}, setCol string, setVal interface{}) {
	t.Helper()
	tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?;", table, setCol, whereCol), setVal, whereVal)
}

func (tb *testBackend) Delete(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}) {
	t.Helper()
	tb.Query(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE %s = ?;", table, whereCol), whereVal)
}

func (tb *testBackend) Query(ctx context.Context, t testing.TB, query string, args ...interface{}) {
	t.Helper()
	logrus.WithFields(logrus.Fields{"query": query, "args": args}).Debug("executing query")
	var result, err = tb.control.Execute(query, args...)
	if err != nil {
		t.Fatalf("error executing query %q: %v", query, err)
	}
	defer result.Close()
}

func uniqueTableID(t testing.TB, extra ...string) string {
	t.Helper()
	var h = sha256.New()
	h.Write([]byte(t.Name()))
	for _, x := range extra {
		h.Write([]byte{':'})
		h.Write([]byte(x))
	}
	var x = binary.BigEndian.Uint32(h.Sum(nil)[0:4])
	return fmt.Sprintf("%d", (x%900000)+100000)
}

func setShutdownAfterCaughtUp(t testing.TB, setting bool) {
	t.Helper()
	var prevSetting = sqlcapture.TestShutdownAfterCaughtUp
	sqlcapture.TestShutdownAfterCaughtUp = setting
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = prevSetting })
}

// TestGeneric runs the generic sqlcapture test suite.
func TestGeneric(t *testing.T) {
	var tb = mysqlTestBackend(t)
	tb.lowerTuningParameters(t)
	tests.Run(context.Background(), t, tb)
}

func TestAlterTable_ChangeColumn(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "27484562"
	var table = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table, [][]interface{}{{1, "aaa"}, {2, "bbb"}})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Rename and change type to varchar, but don't change position
	tb.Insert(ctx, t, table, [][]interface{}{{3, "ccc"}})
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %[1]s CHANGE COLUMN `data` `data_two` VARCHAR(10);", table))
	tb.Insert(ctx, t, table, [][]interface{}{{4, "ddd"}})
	t.Run("capture1", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Rename, preserving the varchar type, but reorder to be the first column
	tb.Insert(ctx, t, table, [][]interface{}{{5, "eee"}})
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %[1]s CHANGE COLUMN `data_two` `data_three` VARCHAR(10) FIRST;", table))
	tb.Insert(ctx, t, table, [][]interface{}{{"fff", 6}})
	t.Run("capture2", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Rename, changing datatype back to text, and move the column back to the end
	tb.Insert(ctx, t, table, [][]interface{}{{"ggg", 7}})
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %[1]s CHANGE COLUMN `data_three` `data` TEXT AFTER `id`;", table))
	tb.Insert(ctx, t, table, [][]interface{}{{8, "hhh"}})
	t.Run("capture3", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestAlterTable_ModifyColumn(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "13419621"
	var table = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, tag TEXT, data TEXT)")
	tb.Insert(ctx, t, table, [][]interface{}{{1, "A", "aaa"}, {2, "B", "bbb"}})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Change type to varchar but don't move it
	tb.Insert(ctx, t, table, [][]interface{}{{3, "C", "ccc"}})
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %[1]s MODIFY COLUMN `tag` VARCHAR(10);", table))
	tb.Insert(ctx, t, table, [][]interface{}{{4, "D", "ddd"}})
	t.Run("capture1", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Preserve varchar type, move to beginning
	tb.Insert(ctx, t, table, [][]interface{}{{5, "E", "eee"}})
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %[1]s MODIFY COLUMN `tag` VARCHAR(10) FIRST;", table))
	tb.Insert(ctx, t, table, [][]interface{}{{"F", 6, "fff"}})
	t.Run("capture2", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Change back to text, move to end
	tb.Insert(ctx, t, table, [][]interface{}{{"G", 7, "ggg"}})
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %[1]s MODIFY COLUMN `tag` TEXT AFTER `data`;", table))
	tb.Insert(ctx, t, table, [][]interface{}{{8, "hhh", "H"}})
	t.Run("capture3", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Preserve text type, move to middle
	tb.Insert(ctx, t, table, [][]interface{}{{9, "iii", "I"}})
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %[1]s MODIFY COLUMN `tag` TEXT AFTER `id`;", table))
	tb.Insert(ctx, t, table, [][]interface{}{{10, "J", "jjj"}})
	t.Run("capture4", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestAlterTable_RenameColumn(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "73330825"
	var table = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table, [][]interface{}{{1, "aaa"}, {2, "bbb"}})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Rename the column
	tb.Insert(ctx, t, table, [][]interface{}{{3, "ccc"}})
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %[1]s RENAME COLUMN `data` TO `data_two`;", table))
	tb.Insert(ctx, t, table, [][]interface{}{{4, "ddd"}})
	t.Run("capture1", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Rename back to the original name
	tb.Insert(ctx, t, table, [][]interface{}{{5, "eee"}})
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %[1]s RENAME COLUMN `data_two` TO `data`;", table))
	tb.Insert(ctx, t, table, [][]interface{}{{6, "fff"}})
	t.Run("capture2", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestAlterTable_AddColumnBasic(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "68678323"
	var table = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table, [][]interface{}{
		{1, "aaa"},
		{2, "bbb"},
	})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Can add a column at the end of the table.
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra_end TEXT;", table))
	tb.Insert(ctx, t, table, [][]interface{}{
		{3, "eee", "extra_end_3"},
		{4, "fff", "extra_end_4"},
	})
	t.Run("at_end", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))
	tb.Insert(ctx, t, table, [][]interface{}{
		{5, "ggg", "extra_end_5"},
		{6, "hhh", "extra_end_6"},
	})
	t.Run("at_end_restart", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Can add a column at the beginning of the table.
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra_start TEXT FIRST;", table))
	tb.Insert(ctx, t, table, [][]interface{}{
		{"extra_start_7", 7, "iii", "extra_end_7"},
		{"extra_start_8", 8, "jjj", "extra_end_8"},
	})
	t.Run("at_first", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))
	tb.Insert(ctx, t, table, [][]interface{}{
		{"extra_start_9", 9, "kkk", "extra_end_9"},
		{"extra_start_10", 10, "lll", "extra_end_10"},
	})
	t.Run("at_first_restart", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Can add a column in the middle of the table, and case sensitivity is not a problem.
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN Extra_MIDDLE TEXT AFTER id;", table))
	tb.Insert(ctx, t, table, [][]interface{}{
		{"extra_start_11", 11, "extra_middle_1", "mmm", "extra_end_11"},
		{"extra_start_12", 12, "extra_middle_2", "nnn", "extra_end_12"},
	})
	t.Run("at_middle", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))
	tb.Insert(ctx, t, table, [][]interface{}{
		{"extra_start_13", 13, "extra_middle_13", "ooo", "extra_end_13"},
		{"extra_start_14", 14, "extra_middle_14", "ppp", "extra_end_14"},
	})
	t.Run("at_middle_restart", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestAlterTable_MultipleAlterations(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "95139670"
	var table = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table, [][]interface{}{
		{1, "aaa"},
		{2, "bbb"},
	})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	tb.Query(ctx, t, fmt.Sprintf(`
		ALTER TABLE %s 
		ADD COLUMN extra_after_id TEXT AFTER id,
		ADD COLUMN extra_first TEXT FIRST,
		DROP COLUMN data,
		ADD COLUMN extra_end TEXT;`,
		table,
	))
	tb.Insert(ctx, t, table, [][]interface{}{
		{"extra_first_3", 3, "extra_after_id_3", "extra_end_3"},
		{"extra_first_4", 4, "extra_after_id_4", "extra_end_4"},
	})

	t.Run("altered", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestAlterTable_AddColumnSetEnum(t *testing.T) {
	t.Run("enum", func(t *testing.T) {
		var tb, ctx = mysqlTestBackend(t), context.Background()
		var uniqueID = "76927424"
		var table = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
		tb.Insert(ctx, t, table, [][]interface{}{
			{1, "aaa"},
			{2, "bbb"},
		})

		var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
		t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

		tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD enumCol ENUM('someValue','anotherValue');;", table))
		tb.Insert(ctx, t, table, [][]interface{}{
			{3, "ccc", "anotherValue"},
			{4, "ddd", "someValue"},
		})
		t.Run("stream", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

		cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))
		tb.Insert(ctx, t, table, [][]interface{}{
			{5, "eee", "someValue"},
			{6, "fff", "someValue"},
		})
		t.Run("restart", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
	})

	t.Run("set", func(t *testing.T) {
		var tb, ctx = mysqlTestBackend(t), context.Background()
		var uniqueID = "14622082"
		var table = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
		tb.Insert(ctx, t, table, [][]interface{}{
			{1, "aaa"},
			{2, "bbb"},
		})

		var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
		t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

		tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD setCol SET('a','b','c');;", table))
		tb.Insert(ctx, t, table, [][]interface{}{
			{3, "ccc", "a,b"},
			{4, "ddd", "b,c"},
		})
		t.Run("stream", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

		cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))
		tb.Insert(ctx, t, table, [][]interface{}{
			{5, "eee", "a,c"},
			{6, "fff", "b,c"},
		})
		t.Run("restart", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
	})
}

func TestAlterTable_DropColumn(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "44468116"
	var table = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table, [][]interface{}{{1, "abc"}, {2, "def"}})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s DROP COLUMN data;", table))
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD other_data TEXT;", table))
	tb.Insert(ctx, t, table, [][]interface{}{{3, "ghi"}, {4, "jkl"}})
	t.Run("stream", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))
	tb.Insert(ctx, t, table, [][]interface{}{{5, "mno"}, {6, "pqr"}})
	t.Run("restart", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestAlterTable_AddEnumColumn(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "30213486"
	var table = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table, [][]interface{}{{1, "aaa"}, {2, "bbb"}})

	t.Run("discover1", func(t *testing.T) { tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN enumcol ENUM('sm', 'med','lg');", table))
	tb.Insert(ctx, t, table, [][]interface{}{
		{3, "eee", "med"},
		{4, "fff", "lg"},
		{5, "ggg", "sm"},
	})
	t.Run("modified", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	t.Run("discover2", func(t *testing.T) { tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })
	cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("rebackfilled", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestAlterTable_AddUnsignedColumn(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "57413089"
	var table = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, table, [][]interface{}{{1, "aaa"}, {2, "bbb"}})

	t.Run("discover1", func(t *testing.T) { tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN uintval BIGINT UNSIGNED;", table))
	tb.Insert(ctx, t, table, [][]interface{}{
		{3, "eee", "17777777777777777777"},
		{4, "fff", "18000000000000000000"},
	})
	t.Run("discover2", func(t *testing.T) { tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })
	t.Run("modified", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("rebackfilled", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

// TestAlterTable_RenameColumnCaseInsensitive verifies that column renames work correctly
// even when the ALTER TABLE query uses different capitalization than the original column
// definition. MySQL always treats column identifiers as case-insensitive.
func TestAlterTable_RenameColumnCaseInsensitive(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "41432361"
	var table = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, dataColumn TEXT)")
	tb.Insert(ctx, t, table, [][]interface{}{{1, "aaa"}, {2, "bbb"}})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Use different capitalization in the rename query
	tb.Insert(ctx, t, table, [][]interface{}{{3, "ccc"}})
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN `DATACOLUMN` TO `new_data`;", table))
	tb.Insert(ctx, t, table, [][]interface{}{{4, "ddd"}})
	t.Run("renamed", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Verify schema discovery still works after the rename
	t.Run("discover", func(t *testing.T) { tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID)) })
}

func TestSkipBackfills(t *testing.T) {
	// Set up three tables with some data in them, a catalog which captures all three,
	// but a configuration which specifies that tables A and C should skip backfilling
	// and only capture new changes.
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueA, uniqueB, uniqueC = "11917332", "20812231", "30443514"
	var tableA = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableB = tb.CreateTable(ctx, t, uniqueB, "(id INTEGER PRIMARY KEY, data TEXT)")
	var tableC = tb.CreateTable(ctx, t, uniqueC, "(id INTEGER PRIMARY KEY, data TEXT)")
	tb.Insert(ctx, t, tableA, [][]interface{}{{1, "one"}, {2, "two"}, {3, "three"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{4, "four"}, {5, "five"}, {6, "six"}})
	tb.Insert(ctx, t, tableC, [][]interface{}{{7, "seven"}, {8, "eight"}, {9, "nine"}})

	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB), regexp.MustCompile(uniqueC))
	cs.EndpointSpec.(*Config).Advanced.SkipBackfills = fmt.Sprintf("%s,%s", tableA, tableC)

	// Run an initial capture, which should only backfill events from table B
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Insert additional data and verify that all three tables report new events
	tb.Insert(ctx, t, tableA, [][]interface{}{{10, "ten"}, {11, "eleven"}, {12, "twelve"}})
	tb.Insert(ctx, t, tableB, [][]interface{}{{13, "thirteen"}, {14, "fourteen"}, {15, "fifteen"}})
	tb.Insert(ctx, t, tableC, [][]interface{}{{16, "sixteen"}, {17, "seventeen"}, {18, "eighteen"}})
	t.Run("main", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

// TestCursorResume sets up a capture with a (string, int) primary key and
// and repeatedly restarts it after each row of capture output.
func TestCursorResume(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "26865190"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(epoch VARCHAR(8), count INTEGER, data TEXT, PRIMARY KEY (epoch, count))")
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{"aaa", 1, "bvzf"}, {"aaa", 2, "ukwh"}, {"aaa", 3, "lntg"}, {"bbb", -100, "bycz"},
		{"bbb", 2, "ajgp"}, {"bbb", 333, "zljj"}, {"bbb", 4096, "lhnw"}, {"bbb", 800000, "iask"},
		{"ccc", 1234, "bikh"}, {"ddd", -10000, "dhqc"}, {"x", 1, "djsf"}, {"y", 1, "iwnx"},
		{"z", 1, "qmjp"}, {"", 0, "xakg"}, {"", -1, "kvxr"}, {"   ", 3, "gboj"},
	})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	// Reduce the backfill chunk size to 1 row. Since the capture will be killed and
	// restarted after each scan key update, this means we'll advance over the keys
	// one by one.
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 1
	var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
	cupaloy.SnapshotT(t, summary)
}

// TestComplexDataset tries to throw together a bunch of different bits of complexity
// to synthesize something vaguely "realistic". It features a multiple-column primary
// key, a dataset large enough that the initial table scan gets divided across many
// "chunks", two connector restarts at different points in the initial table scan, and
// some concurrent modifications to row ranges already-scanned and not-yet-scanned.
func TestComplexDataset(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "56015963"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(year INTEGER, state VARCHAR(2), fullname VARCHAR(64), population INTEGER, PRIMARY KEY (year, state))")
	tests.LoadCSV(ctx, t, tb, tableName, "statepop.csv", 0)
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	// Reduce the backfill chunk size to 10 rows for this test.
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 10

	t.Run("init", func(t *testing.T) {
		var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)

		// Rewind the backfill state to a specific reproducible point
		var state sqlcapture.PersistentState
		require.NoError(t, json.Unmarshal(cs.Checkpoint, &state))
		state.Streams["test%2FComplexDataset_56015963"].BackfilledCount = 130
		state.Streams["test%2FComplexDataset_56015963"].Mode = sqlcapture.TableStateUnfilteredBackfill
		state.Streams["test%2FComplexDataset_56015963"].Scanned = []byte{0x16, 0x07, 0x94, 0x01, 0x4e, 0x56, 0x00}
		var bs, err = json.Marshal(&state)
		require.NoError(t, err)
		cs.Checkpoint = bs
	})

	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // An insert prior to the first restart, which will be reported once replication begins
		{1970, "XX", "No Such State", 12345},  // An insert between the two restarts, which will be visible in the table scan and should be filtered during replication
		{1990, "XX", "No Such State", 123456}, // An insert after the second restart, which will be visible in the table scan and should be filtered during replication
	})
	t.Run("restart1", func(t *testing.T) {
		var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)

		// Rewind the backfill state to a specific reproducible point
		var state sqlcapture.PersistentState
		require.NoError(t, json.Unmarshal(cs.Checkpoint, &state))
		state.Streams["test%2FComplexDataset_56015963"].BackfilledCount = 230
		state.Streams["test%2FComplexDataset_56015963"].Mode = sqlcapture.TableStateUnfilteredBackfill
		state.Streams["test%2FComplexDataset_56015963"].Scanned = []byte{0x16, 0x07, 0xbc, 0x01, 0x4e, 0x48, 0x00}
		var bs, err = json.Marshal(&state)
		require.NoError(t, err)
		cs.Checkpoint = bs
	})

	tb.Query(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE state = 'XX';", tableName))
	tb.Insert(ctx, t, tableName, [][]interface{}{
		{1930, "XX", "No Such State", 1234},   // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1970, "XX", "No Such State", 12345},  // Deleting/reinserting this row will be reported since they happened after that portion of the table was scanned
		{1990, "XX", "No Such State", 123456}, // Deleting/reinserting this row will be filtered since this portion of the table has yet to be scanned
	})
	t.Run("restart2", func(t *testing.T) {
		var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
		cupaloy.SnapshotT(t, summary)
	})
}

// TestNonCommitFinalQuery sets up a situation where the capture needs to resume
// and stream to a fence, but the database is idle and the final event in the WAL
// is not a transaction commit.
//
// This can be a tricky situation for a positional fence mechanism to handle.
func TestNonCommitFinalQuery(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "82446880"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, data TEXT)")

	tb.Insert(ctx, t, tableName, [][]any{{0, "aaa"}, {1, "bbb"}})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
	tests.RunCapture(ctx, t, cs)
	cs.Reset()

	// The ALTER TABLE query here should be a no-op
	tb.Insert(ctx, t, tableName, [][]any{{2, "ccc"}, {3, "ddd"}})
	tb.Query(ctx, t, fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra TEXT;", tableName))
	tests.VerifiedCapture(ctx, t, cs)
}

// TestFeatureFlagTinyintAsBool exercises the "tinyint1_as_bool" feature flag
// in both discovery and captures.
func TestFeatureFlagTinyintAsBool(t *testing.T) {
	var tb, ctx = mysqlTestBackend(t), context.Background()
	var uniqueID = "21925183"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER PRIMARY KEY, v_bool BOOLEAN, v_tinyint TINYINT)")

	tb.Insert(ctx, t, tableName, [][]any{
		{1, true, 1}, {2, false, 0}, {3, true, 2}, {4, false, 127},
		{5, true, -128}, {6, nil, 1}, {7, true, nil}, {8, nil, nil},
	})

	for _, tc := range []struct {
		name  string
		flags string
	}{
		{"Default", ""},
		{"Enabled", "tinyint1_as_bool"},
		{"Disabled", "no_tinyint1_as_bool"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cs = tb.CaptureSpec(ctx, t)
			cs.EndpointSpec.(*Config).Advanced.FeatureFlags = tc.flags

			t.Run("Discovery", func(t *testing.T) {
				cs.VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
			})

			t.Run("Capture", func(t *testing.T) {
				cs.Bindings = tests.DiscoverBindings(ctx, t, tb, regexp.MustCompile(uniqueID))
				cs.Validator = &st.OrderedCaptureValidator{}
				sqlcapture.TestShutdownAfterCaughtUp = true
				t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

				cs.Capture(ctx, t, nil)
				cupaloy.SnapshotT(t, cs.Summary())
			})
		})
	}
}
