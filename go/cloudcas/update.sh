#!/usr/bin/env bash
# Refreshes the managed-database CA bundles that 'verify_identity' trusts in
# addition to the system root store. Run from any directory, then review the
# diff: a removed certificate breaks every server that still chains to it.
set -euo pipefail
cd "$(dirname "$0")"

# Amazon RDS and Aurora, all commercial regions, and AWS GovCloud (US). China
# regions have a bundle of their own, which is not included.
# https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html
curl -sSfo amazon-rds-global.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
curl -sSfo amazon-rds-govcloud.pem https://truststore.pki.us-gov-west-1.rds.amazonaws.com/global/global-bundle.pem

# Google Cloud SQL instances using the shared CA (serverCaMode GOOGLE_MANAGED_CAS_CA),
# all regions. The default per-instance CA is unique to each instance and cannot be bundled.
# https://cloud.google.com/sql/docs/mysql/manage-ssl-instance
curl -sSfo google-cloud-sql-global.pem https://storage.googleapis.com/cloudsql-ca-bundles/global.pem

grep -c 'BEGIN CERTIFICATE' ./*.pem
