-- Runs once, on the first start of the Postgres container (local only; the
-- passwords are local examples, not secrets).
--   wallet_owner    owns the schema and runs the migrations
--   wallet_app      privilege group of the service (no login)
--   wallet_service  login used by the service instances
CREATE ROLE wallet_owner LOGIN PASSWORD 'wallet_owner';
CREATE ROLE wallet_app NOLOGIN;
CREATE ROLE wallet_service LOGIN PASSWORD 'wallet_service' IN ROLE wallet_app;

CREATE DATABASE wallet OWNER wallet_owner;
REVOKE ALL ON DATABASE wallet FROM PUBLIC;
GRANT CONNECT ON DATABASE wallet TO wallet_app;
