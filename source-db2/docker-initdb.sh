#!/bin/bash
set -euo pipefail

# Run as root after the image reports "Setup has completed". On emulated amd64
# the image can finish without creating sqllib, so finish instance setup here.
if [[ -f /tmp/flow-db2-ready ]]; then
    exit 0
fi
db2icrt=(/opt/ibm/db2/V*/instance/db2icrt)
if [[ ${#db2icrt[@]} -ne 1 || ! -x "${db2icrt[0]}" ]]; then
    echo "Expected one Db2 instance installer at /opt/ibm/db2/V*/instance/db2icrt" >&2
    exit 1
fi
"${db2icrt[0]}" -update-instance-if-exists -u db2fenc1 -nosharedgroup db2inst1

su - db2inst1 -c 'bash -s' <<'DB2'
source "$HOME/sqllib/db2profile"
set -euo pipefail
# The SQL literals below are UTF-8, regardless of the instance user's locale.
export DB2CODEPAGE=1208
cd "$HOME"

db2start > /dev/null 2>&1 || true
mkdir -p "$HOME/arch"
db2 "CREATE DATABASE TESTDB"
db2 "UPDATE DB CFG FOR TESTDB USING LOGARCHMETH1 DISK:$HOME/arch"
# This disposable fixture does not need to retain a recoverable backup.
db2 "BACKUP DB TESTDB TO /dev/null"
db2 "CONNECT TO TESTDB"
db2 -stvf /dev/stdin <<'SQL'
CREATE TABLE DB2INST1.ORDERS (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(50), QTY INT, PRICE DECIMAL(10,2), CREATED TIMESTAMP, NOTE VARCHAR(200)) DATA CAPTURE CHANGES;
CREATE TABLE DB2INST1.LINES (ORDER_ID INT NOT NULL, LINE_NO SMALLINT NOT NULL, SKU CHAR(8), QTY INT, PRIMARY KEY (ORDER_ID, LINE_NO)) DATA CAPTURE CHANGES;
CREATE TABLE DB2INST1.CDC_BI (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(50), QTY INT) DATA CAPTURE CHANGES;
CREATE TABLE DB2INST1.CDC_DI (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(50), QTY INT) DATA CAPTURE CHANGES;
CREATE TABLE DB2INST1.FLOW_WATERMARKS (SLOT VARCHAR(1000) NOT NULL PRIMARY KEY, WATERMARK VARCHAR(1000)) DATA CAPTURE CHANGES;
CREATE TABLE DB2INST1.TEST_CAPTURE ("id" INT NOT NULL PRIMARY KEY, "canary" VARCHAR(50) NOT NULL, QTY INT, PRICE DECIMAL(10,2), CREATED TIMESTAMP, NOTE VARCHAR(200), CONTENT DBCLOB(1M)) DATA CAPTURE CHANGES;
SQL

cat > setup.asnclp <<'ASNCLP'
SET SERVER CAPTURE TO DB TESTDB;
SET RUN SCRIPT NOW STOP ON SQL ERROR ON;
CREATE CONTROL TABLES FOR CAPTURE SERVER;
CREATE REGISTRATION (DB2INST1.ORDERS) DIFFERENTIAL REFRESH STAGE CDORDERS COLS ALL IMAGE BOTH PREFIX X;
CREATE REGISTRATION (DB2INST1.LINES) DIFFERENTIAL REFRESH STAGE CDLINES OPTIONS UPDATE AS DELETE INSERT ON;
CREATE REGISTRATION (DB2INST1.CDC_BI) DIFFERENTIAL REFRESH STAGE CDCDC_BI COLS ALL IMAGE BOTH PREFIX X;
CREATE REGISTRATION (DB2INST1.CDC_DI) DIFFERENTIAL REFRESH STAGE CDCDC_DI OPTIONS UPDATE AS DELETE INSERT ON;
CREATE REGISTRATION (DB2INST1.FLOW_WATERMARKS) DIFFERENTIAL REFRESH STAGE CDFLOW_WATERMARKS;
CREATE REGISTRATION (DB2INST1.TEST_CAPTURE) DIFFERENTIAL REFRESH STAGE CDTEST_CAPTURE OPTIONS UPDATE AS DELETE INSERT ON;
ASNCLP
if ! asnclp -f setup.asnclp > asnclp.out 2>&1; then
    cat asnclp.out >&2
    exit 1
fi
cat asnclp.out
if grep -Eq 'ASN[0-9]+E' asnclp.out; then
    exit 1
fi

db2 "UPDATE ASN.IBMSNAP_CAPPARMS SET COMMIT_INTERVAL = 2, SLEEP_INTERVAL = 1"
nohup asncap capture_server=TESTDB capture_schema=ASN startmode=warmsi > asncap.out 2>&1 < /dev/null &

wait_log() {
    local logfile="$1" message="$2"
    for ((attempt = 0; attempt < 120; attempt++)); do
        if grep -q "$message" "$logfile" 2>/dev/null; then
            return 0
        fi
        sleep 2
    done
    echo "Timed out waiting for $message in $logfile" >&2
    cat asncap.out "$logfile" >&2
    return 1
}

wait_log asncap.out ASN0109I
# There is no Apply program in the fixture to send CAPSTART signals.
db2 "UPDATE ASN.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'"
asnccmd capture_server=TESTDB capture_schema=ASN reinit
wait_log db2inst1.TESTDB.ASN.CAP.log ASN0023I

db2 -stvf /dev/stdin <<'SQL'
INSERT INTO DB2INST1.ORDERS VALUES (1, 'apple', 1, 1.50, '2026-01-02-03.04.05.000000', 'first'), (2, 'pear', 2, 2.25, '2026-01-02-03.04.06.000000', NULL), (3, 'plum', 3, 3.00, '2026-01-02-03.04.07.000000', 'third');
INSERT INTO DB2INST1.LINES VALUES (1, 1, 'SKU-A', 5), (1, 2, 'SKU-B', 6), (2, 1, 'SKU-C', 7);
INSERT INTO DB2INST1.TEST_CAPTURE VALUES (1, 'apple', 1, 1.50, '2026-01-02-03.04.05.000000', 'first', DBCLOB('A')), (2, 'pear', 2, 2.25, '2026-01-02-03.04.06.000000', NULL, DBCLOB('é')), (3, 'plum', 3, 3.00, '2026-01-02-03.04.07.000000', 'third', DBCLOB('漢🌍')), (4, 'kiwi', NULL, 4.00, '2026-01-02-03.04.08.000000', 'fourth', NULL);
INSERT INTO DB2INST1.FLOW_WATERMARKS VALUES ('fixture', 'ready');
SQL

wait_published() {
    local source_table="$1" predicate="$2" cd_table count
    # ASNCLP chooses the CD schema independently of the source schema.
    if ! cd_table=$(db2 -x "SELECT '\"' || RTRIM(CD_OWNER) || '\".\"' || RTRIM(CD_TABLE) || '\"' FROM ASN.IBMSNAP_REGISTER WHERE SOURCE_OWNER = 'DB2INST1' AND SOURCE_TABLE = '$source_table' AND STATE = 'A'"); then
        echo "Unable to read the registration for DB2INST1.$source_table: $cd_table" >&2
        return 1
    fi
    cd_table="${cd_table//[[:space:]]/}"
    if [[ -z "$cd_table" ]]; then
        echo "No active SQL Replication registration for DB2INST1.$source_table" >&2
        return 1
    fi

    echo "Waiting for DB2INST1.$source_table changes in $cd_table"
    for ((attempt = 0; attempt < 120; attempt++)); do
        if ! count=$(db2 -x "SELECT COUNT(*) FROM $cd_table C, ASN.IBMSNAP_REGISTER R WHERE R.GLOBAL_RECORD = 'Y' AND ($predicate) AND C.IBMSNAP_COMMITSEQ <= R.SYNCHPOINT"); then
            echo "Unable to check publication in $cd_table: $count" >&2
            return 1
        fi
        if [[ "${count//[[:space:]]/}" == 1 ]]; then
            return 0
        fi
        sleep 2
    done
    echo "ASN Capture did not publish $cd_table through the global SYNCHPOINT" >&2
    cat asncap.out db2inst1.TESTDB.ASN.CAP.log >&2
    return 1
}

wait_published ORDERS 'ID = 3'
wait_published LINES 'ORDER_ID = 2'
wait_published TEST_CAPTURE '"id" = 4'
wait_published FLOW_WATERMARKS "SLOT = 'fixture'"
db2 "CONNECT RESET"
DB2

touch /tmp/flow-db2-ready
