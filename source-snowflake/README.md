Flow Snowflake CDC Source Connector
===================================

This is a connector which captures changes to Snowflake tables.

It does this using [streams](https://docs.snowflake.com/en/user-guide/streams-intro),
by periodically copying all new changes (from the stream corresponding to a particular
source table) into a connector-managed staging table and then capturing the contents of
that staging table into Flow.

Useful commands:

    $ docker build -t ghcr.io/estuary/source-snowflake:local -f source-snowflake/Dockerfile .
    $ flowctl raw discover --source acmeCo/flow.yaml
    $ flowctl raw capture --source acmeCo/flow.yaml

Example `flow.yaml` for discovery:

    captures:
      acmeCo/source-snowflake:
        endpoint:
          connector:
            image: "ghcr.io/estuary/source-snowflake:v1"
            config:
              credentials:
                auth_type: jwt
                user: snowflake_user
                privateKey: |
                  -----BEGIN PRIVATE KEY-----
                  MIIEv....
                  ...
                  ...
                  ...
                  ...
                  ...
                  -----END PRIVATE KEY-----
              host: bn92689.us-central1.gcp.snowflakecomputing.com
              database: CONNECTOR_TESTING
              warehouse: COMPUTE_WH
        bindings: []