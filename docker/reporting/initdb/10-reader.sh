#!/usr/bin/env bash
# Runs only during first initialization of a NEW reporting PostgreSQL volume.
# A subshell also keeps settings private if the image entrypoint sources this file.
(
    set -euo pipefail
    [[ "${POSTGRES_DB:-}" == energy_reporting ]] || exit 1
    [[ "${POSTGRES_USER:-}" == energy_reporting_owner ]] || exit 1

    # The root bootstrap stages a postgres-owned copy in private tmpfs.
    reporting_reader_password=$(cat /run/reporting-secrets/reporting_reader_password)
    if [[ -z "$reporting_reader_password" || "$reporting_reader_password" == *$'\n'* ]]; then
        printf '%s\n' 'Reporting reader password must be nonempty and one line.' >&2
        exit 1
    fi

    # Supply the secret through the child environment, never process arguments.
    # Suppress SQL error text here because it could contain a password literal.
    if ! REPORTING_READER_PASSWORD="$reporting_reader_password" \
        psql -X --no-password --host /var/run/postgresql --port 5432 \
        --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        --set ON_ERROR_STOP=1 >/dev/null 2>&1 <<'SQL'
\getenv reader_password REPORTING_READER_PASSWORD
BEGIN;
CREATE ROLE energy_reporting_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS PASSWORD :'reader_password';
REVOKE ALL ON DATABASE energy_reporting FROM PUBLIC;
GRANT CONNECT ON DATABASE energy_reporting TO energy_reporting_reader;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
COMMIT;
SQL
    then
        unset reporting_reader_password
        printf '%s\n' 'Reporting reader initialization failed; preserve the volume for review.' >&2
        exit 1
    fi
    unset reporting_reader_password
)
