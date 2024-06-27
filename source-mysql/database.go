package main

import (
	"fmt"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/sirupsen/logrus"
)

// queryDatabaseVersion examines the server version string to figure out what product
// and release version we're talking to, and saves the results for later use.
func (db *mysqlDatabase) queryDatabaseVersion() error {
	var version string
	var results, err = db.conn.Execute(`SELECT @@GLOBAL.version;`)
	if err != nil {
		return fmt.Errorf("unable to query database version: %w", err)
	} else if len(results.Values) != 1 || len(results.Values[0]) != 1 {
		return fmt.Errorf("unable to query database version: malformed response")
	}

	db.versionString = string(results.Values[0][0].AsString())
	db.versionProduct = "MySQL"
	if strings.Contains(strings.ToLower(db.versionString), "mariadb") {
		db.versionProduct = "MariaDB"
	}
	major, minor, err := sqlcapture.ParseVersion(version)
	if err != nil {
		return fmt.Errorf("unable to parse database version from %q: %w", version, err)
	}
	db.versionMajor = major
	db.versionMinor = minor

	logrus.WithFields(logrus.Fields{
		"version": db.versionString,
		"product": db.versionProduct,
		"major":   db.versionMajor,
		"minor":   db.versionMinor,
	}).Info("queried database version")
	return nil
}
