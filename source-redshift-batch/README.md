Redshift Batch Source Connector
===============================

Redshift external connectivity setup:

1. Set 'Publicly Accessible' to 'On' for the workgroup
2. The associated VPC Security Group needs an Inbound Rule permitting Redshift
   traffic from `0.0.0.0/0` or from the Estuary public IP.
3. The associated VPC Route Table needs a route for traffic to `0.0.0.0/0` or
   the Estuary public IP to egress via an Internet Gateway.
4. If the VPC uses network ACLs, they also need to permit inbound and outbound
   Redshift traffic.

Example user setup:

    $ psql 'postgres://admin:Secret1234@default-workgroup.123456789123.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
    dev=# CREATE USER flow_capture WITH PASSWORD 'Secret1234';
    dev=# CREATE SCHEMA test;
    dev=# ALTER DEFAULT PRIVILEGES FOR USER admin IN SCHEMA test GRANT ALL ON TABLES TO flow_capture;
    dev=# GRANT ALL ON SCHEMA test TO flow_capture;