package version

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

// Info contains version and edition information about the SQL Server instance.
type Info struct {
	EngineEdition     int    // Numeric engine edition code
	EngineEditionName string // Human-readable engine edition name
	ProductVersion    string // Full version string (e.g., "14.0.3456.2")
	ProductLevel      string // Release level (e.g., "RTM", "SP1", "CTP1")
	Edition           string // Full edition name (e.g., "Standard Edition (64-bit)")
	MajorVersion      int    // Parsed major version number (not set when IsAzureManaged is true)
	MinorVersion      int    // Parsed minor version number (not set when IsAzureManaged is true)
	IsAzureManaged    bool   // Whether this is an Azure managed instance
}

// EngineEditionNames maps EngineEdition codes to human-readable names.
// See https://learn.microsoft.com/en-us/sql/t-sql/functions/serverproperty-transact-sql
var EngineEditionNames = map[int]string{
	1:  "Personal or Desktop Engine",
	2:  "Standard",
	3:  "Enterprise",
	4:  "Express",
	5:  "Azure SQL Database",
	6:  "Azure Synapse Analytics",
	8:  "Azure SQL Managed Instance",
	9:  "Azure SQL Edge",
	11: "Azure Synapse serverless SQL pool / Microsoft Fabric",
	12: "Microsoft Fabric SQL database",
}

// AzureManagedEditions identifies EngineEdition codes for Azure managed instances.
// Per https://learn.microsoft.com/en-us/sql/t-sql/functions/version-transact-sql-configuration-functions,
// the product version reported by @@VERSION is incorrect for these editions. They are managed
// services that are always up-to-date, so we skip version checking for them.
var AzureManagedEditions = map[int]bool{
	5:  true, // Azure SQL Database
	6:  true, // Azure Synapse Analytics
	8:  true, // Azure SQL Managed Instance
	11: true, // Azure Synapse serverless SQL pool / Microsoft Fabric
	12: true, // Microsoft Fabric SQL database
}

// Query queries the SQL Server instance for version and edition information.
func Query(ctx context.Context, conn *sql.DB) (*Info, error) {
	var info Info

	// Query engine edition
	if err := conn.QueryRowContext(ctx, `SELECT SERVERPROPERTY('EngineEdition');`).Scan(&info.EngineEdition); err != nil {
		return nil, fmt.Errorf("unable to query 'EngineEdition' server property: %w", err)
	}
	info.EngineEditionName = EngineEditionNames[info.EngineEdition]
	if info.EngineEditionName == "" {
		info.EngineEditionName = fmt.Sprintf("Unknown (%d)", info.EngineEdition)
	}
	info.IsAzureManaged = AzureManagedEditions[info.EngineEdition]

	// Query edition name and product level
	var edition, productLevel sql.NullString
	if err := conn.QueryRowContext(ctx, `SELECT SERVERPROPERTY('Edition'), SERVERPROPERTY('ProductLevel');`).Scan(&edition, &productLevel); err != nil {
		log.WithField("err", err).Debug("unable to query 'Edition' and 'ProductLevel' server properties")
	} else {
		info.Edition = edition.String
		info.ProductLevel = productLevel.String
	}

	// Query product version (skip for Azure managed instances where it may be inaccurate)
	if !info.IsAzureManaged {
		var version sql.NullString
		if err := conn.QueryRowContext(ctx, `SELECT SERVERPROPERTY('productversion');`).Scan(&version); err != nil {
			log.WithField("err", err).Debug("unable to query 'productversion' server property")
		} else if version.Valid && version.String != "" {
			info.ProductVersion = version.String
			if major, minor, err := sqlcapture.ParseVersion(version.String); err == nil {
				info.MajorVersion = major
				info.MinorVersion = minor
			}
		}
	}

	return &info, nil
}
