-- Initialization script for source-oracle-batch test database
-- This runs automatically when the container is first created

-- Create common user for batch testing (must use C## prefix for common users in CDB)
CREATE USER C##FLOW_TEST_BATCH IDENTIFIED BY "FLOW123" CONTAINER=ALL;

-- Grant basic privileges for batch testing
GRANT CREATE SESSION TO C##FLOW_TEST_BATCH CONTAINER=ALL;
GRANT CREATE TABLE TO C##FLOW_TEST_BATCH CONTAINER=ALL;
GRANT CREATE VIEW TO C##FLOW_TEST_BATCH CONTAINER=ALL;
GRANT UNLIMITED TABLESPACE TO C##FLOW_TEST_BATCH CONTAINER=ALL;
