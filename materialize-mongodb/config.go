package main

import (
	"fmt"

  "net/url"
)

type config struct {
	Address  string `json:"address" jsonschema:"title=Address,description=Host and port of the database." jsonschema_extras:"order=0"`
	User     string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string `json:"database" jsonschema:"title=Database,description=Name of the database to capture from." jsonschema_extras:"order=3"`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"password", c.Password},
		{"database", c.Database},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	var uri, err = url.Parse(c.Address)
	// mongodb+srv:// urls do not support port
	if err == nil && uri.Scheme == "mongodb+srv" && uri.Port() != "" {
		return fmt.Errorf("`mongodb+srv://` addresses do not support specifying the port.")
	}

	return nil
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI() string {
	var address = c.Address
	var uri, err = url.Parse(address)

	if err != nil || uri.Scheme == "" || uri.Host == "" {
		uri = &url.URL{
			Scheme: "mongodb",
			Host:   address,
		}
	}

	uri.User = url.UserPassword(c.User, c.Password)

	if c.Database != "" {
		uri.Path = "/" + c.Database
	}

	return uri.String()
}

type resource struct {
	Database   string `json:"database" jsonschema=title=Database name"`
	Collection string `json:"collection" jsonschema:"title=Collection name"`
	DeltaUpdates bool `json:"delta_updates,omitempty" jsonschema:"title=Delta updates,default=false"`
}

func (r resource) Validate() error {
	if r.Collection == "" {
		return fmt.Errorf("collection is required")
	}

	if r.Database == "" {
		return fmt.Errorf("database is required")
	}
	return nil
}
