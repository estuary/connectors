package main

import (
	"fmt"

	"net/url"
)

type sshForwarding struct {
	SSHEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SSHForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

type config struct {
	Address  string `json:"address" jsonschema:"title=Address" jsonschema_description:"The connection URI for your database without the username and password. For example mongodb://my-mongo.test?authSource=admin." jsonschema_extras:"order=0"`
	User     string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string `json:"database" jsonschema:"title=Database,description=Name of the database to materialize to." jsonschema_extras:"order=3"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
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
		return fmt.Errorf("`mongodb+srv://` addresses do not support specifying the port")
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

	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:27017 to address
	// through the bastion server, so we use the tunnel's address.
	if c.NetworkTunnel != nil && c.NetworkTunnel.SSHForwarding != nil && c.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		uri.Host = "localhost:27017"
	}

	return uri.String()
}

type resource struct {
	Collection   string `json:"collection" jsonschema:"title=Collection name" jsonschema_extras:"x-collection-name=true"`
	DeltaUpdates bool   `json:"delta_updates,omitempty" jsonschema:"title=Delta updates,default=false"`
}

func (r resource) Validate() error {
	if r.Collection == "" {
		return fmt.Errorf("collection is required")
	}
	return nil
}
