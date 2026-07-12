#!/bin/bash
# Runs once against a fresh data directory (docker-entrypoint-initdb.d convention, executed by the
# official postgres image before it reports healthy). $POSTGRES_USER is the initdb superuser and is
# only ever used by the `migrate` service to run Alembic (CREATE/ALTER/DROP TABLE). Everything that
# runs after migrations (the API, the pipeline) connects as this unprivileged role instead: it can
# read and write rows but cannot touch the schema, so a compromised app container can't drop a table.
set -euo pipefail

: "${DEEPSKY_DB_APP_USER:?DEEPSKY_DB_APP_USER must be set}"
: "${DEEPSKY_DB_APP_PASSWORD:?DEEPSKY_DB_APP_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
	CREATE ROLE "$DEEPSKY_DB_APP_USER" WITH LOGIN PASSWORD '$DEEPSKY_DB_APP_PASSWORD';
	GRANT CONNECT ON DATABASE "$POSTGRES_DB" TO "$DEEPSKY_DB_APP_USER";
	GRANT USAGE ON SCHEMA public TO "$DEEPSKY_DB_APP_USER";

	-- Covers tables that exist yet (there are none on a fresh database) and, via ALTER DEFAULT
	-- PRIVILEGES, every table the migrate service's Alembic run creates afterwards: Postgres scopes
	-- default privileges to the role that creates the object, and migrate connects as
	-- $POSTGRES_USER, so this is set for that same role.
	GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "$DEEPSKY_DB_APP_USER";
	GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "$DEEPSKY_DB_APP_USER";
	ALTER DEFAULT PRIVILEGES FOR ROLE "$POSTGRES_USER" IN SCHEMA public
		GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "$DEEPSKY_DB_APP_USER";
	ALTER DEFAULT PRIVILEGES FOR ROLE "$POSTGRES_USER" IN SCHEMA public
		GRANT USAGE, SELECT ON SEQUENCES TO "$DEEPSKY_DB_APP_USER";
SQL

echo "Created restricted role '$DEEPSKY_DB_APP_USER' (DML only, no schema rights)"
