# Canonical reporting setup

`docker-compose.yml` is the repository's authoritative definition for a **new**
local reporting installation. It contains PostgreSQL, Metabase and nginx. It
does not manage, adopt or replace an existing installation.

**Verified scope:** on 5 November 2026, an isolated `energy_reporting` installation
initialized empty PostgreSQL and Metabase volumes, loaded the static synthetic
sample, and rendered all six questions in Chrome. The root-stage tmpfs secret
delivery and SELECT-only reader restrictions passed runtime checks. Metabase
administrator, connection, questions and dashboard were created through explicit
API setup steps; Compose does not automate that provisioning.

This verifies a fresh **reporting environment**, not a fresh ETL/dbt/Airflow
deployment. The run used cached pinned `linux/amd64` images on Docker Desktop
with WSL. Pulling images on a new machine and other architectures remain unverified.
A subsequent controlled stop/start persistence check passed separately; it was
not part of the initial fresh-install run.

## Recorded fresh-install verification

| Check | Recorded result |
|---|---|
| Images | PostgreSQL 18.6, Metabase v0.50.27, nginx 1.30.6 |
| Compose | 5.6.1; one successful corrected fresh-volume startup; all three services healthy |
| Initialization | `10-reader.sh` and `20-fixture.sql` completed |
| Sample | Six monthly rows and one reconciliation row matched the source SQL |
| Secret delivery | PostgreSQL `999:999`; tmpfs directory `0700`, runtime secret `0600`; host secret remained `0600` |
| Unauthorized secret access | UIDs 65534, 2000 and 101 denied |
| Reader | SELECT succeeded; INSERT, UPDATE, DELETE, TRUNCATE, CREATE TABLE, CREATE SCHEMA and TEMP denied with SQLSTATE `42501` |
| Saved-question row counts | 3, 2, 1, 2, 1, 4 in file order |
| Browser | Chrome 156.0.8078.80; four charts and two tables visible; all six responses matched independent SQL; no query, network or JavaScript errors |

These are recorded results, not assertions that every future checkout or
deployment has been tested. Original screenshots, credentials and private verification
scripts are excluded from the public repository.

## Architecture and isolation

| Service | Persistent state | Exposure |
|---|---|---|
| `reporting` | PostgreSQL 18 reporting tables in `reporting_data` | Internal backend only; no host database port |
| `metabase` | H2 application database in `metabase_data` | Internal backend only; no host application port |
| `proxy` | Read-only `docker/nginx.conf`; temporary files in tmpfs | `127.0.0.1:3002` by default |

Use project name `energy_reporting`. Compose scopes both volumes and both
networks to this project; none is external or mapped to an existing resource.
Only nginx joins the non-internal frontend network. The database and Metabase
share the internal backend. Dependencies wait for PostgreSQL health, then
Metabase health; nginx's health check also reaches the Metabase health endpoint.

Images are pinned to the demonstrated PostgreSQL 18.6, Metabase v0.50.27 and
nginx 1.30.6 image digests, for `linux/amd64`. Other architectures may require Docker
emulation and have not been verified. Image upgrades and production deployment
require their own review. H2 application storage is retained here for the local
demonstration, not presented as a production Metabase deployment recommendation.

## Configuration and private inputs

Requirements: Docker Desktop with WSL integration, modern Docker Compose
(2.33.1+ for `gw_priority`), Bash and available loopback port 3002. Compose 5.6.1
was used for configuration validation. Work from the repository root.
`.gitattributes` keeps the Bash scripts LF-terminated on Windows checkouts.
Check them with `bash -n` before provisioning if files were copied by other tools.

For a **new** local installation:

1. Copy `.env.example` to the ignored `.env.reporting`, without overwriting an
   existing file. Only the `REPORTING_*` entries are needed. No EIA key,
   `POSTGRES_DSN`, dbt configuration or historical credential is required.
2. Create a private directory on a filesystem that enforces Linux permissions,
   owned by you with mode `0700`. Enter two new, nonempty one-line passwords
   privately: an owner password and a reader password. Write files with mode
   `0600`. Use strong passwords from a password manager or secure generator.
   Do not copy old credentials or replace existing passwords.
3. Set `REPORTING_OWNER_PASSWORD_FILE` and `REPORTING_READER_PASSWORD_FILE` to
   those files. Absolute paths are supported. The template's ignored `.secrets`
   paths work only when that directory enforces the required permissions.
   Passwords are not Compose variables and must not appear in `.env.reporting`.

On WSL with a checkout under `/mnt/c`, use a private directory on the WSL Linux
filesystem, such as `$HOME/.local/share/energy-reporting/secrets`, and put its
absolute file paths in ignored `.env.reporting`. DrvFS without metadata support
does not enforce `chmod 0600`; do not loosen permissions or copy secrets onto
that mount. Files outside the checkout stay outside Git; `.secrets/` and
`.env.reporting` are ignored for checkouts that can use them securely.

For example, this Bash block refuses existing files and keeps input off-screen
and out of process arguments. Run it only during initial credential creation:

```bash
(
  set -euo pipefail
  umask 077
  reporting_secret_dir="${XDG_DATA_HOME:-$HOME/.local/share}/energy-reporting/secrets"
  test ! -L "$reporting_secret_dir"
  mkdir -p "$reporting_secret_dir"
  chmod 700 "$reporting_secret_dir"
  test "$(stat -c '%u' "$reporting_secret_dir")" = "$(id -u)"
  test "$(stat -c '%a' "$reporting_secret_dir")" = 700
  set -o noclobber
  for reporting_role in owner reader; do
    reporting_file="$reporting_secret_dir/reporting_${reporting_role}_password"
    test ! -e "$reporting_file"
    read -r -s -p "New reporting ${reporting_role} password: " reporting_password
    printf '\n'
    test -n "$reporting_password"
    printf '%s' "$reporting_password" > "$reporting_file"
    unset reporting_password
    test "$(stat -c '%a' "$reporting_file")" = 600
  done
)
```

Do not use shell tracing or print password files. Check exclusions with
`git check-ignore .env.reporting .secrets/reporting_owner_password`.

### Secret delivery inside PostgreSQL

Compose file-backed secrets are bind mounts; declaring a container `uid`, `gid`
or `mode` does not remap their ownership. Do not make them world-readable. A
host ACL or matching group would require verified Docker Desktop ACL forwarding
and matching container identities; neither is assumed by this setup.

`docker/reporting/entrypoint.sh` runs briefly as root, verifies a `0600` source
file and a dedicated tmpfs mount, and copies only the reader secret into that
tmpfs using the image's `postgres` account by name. The directory is `0700` and
the copy is `0600`. It then executes the unchanged official image entrypoint,
which drops to `postgres` before running initialization and the database server.
PostgreSQL is not configured to run permanently as root.

| Consumer | Reader-secret access |
|---|---|
| Host operator | Original private file; owner-only access |
| PostgreSQL root bootstrap | Read-only source bind; stages the tmpfs copy |
| PostgreSQL OS user (`999:999` in the pinned image) | Private tmpfs copy; init script passes it to psql through its child environment |
| Metabase (`2000:2000` in the demonstrated image) | No mounted password file; its reader connection is configured privately during setup |
| nginx (`101:101`) | No reader-secret mount or value |

The password is not a Compose environment value, command-line argument, SQL
concatenation or committed file. psql reads its child environment with `\getenv`
and quotes it as a SQL literal. Credential-bearing SQL error text is suppressed.
Docker operators/root remain trusted. The tmpfs copy disappears when the
container stops and is recreated on the next start; it is not a database backup.
Changing a password file does not rotate an already initialized database role.

The corrected path passed runtime checks during the recorded fresh installation.
For your installation, verify the runtime directory is tmpfs, owned by `postgres`,
mode `0700`, and its secret is owned by `postgres`, mode `0600`. Use permission-only
probes, without printing the file:

```bash
docker exec --user postgres energy_reporting-reporting-1 sh -c \
  'test -s /run/reporting-secrets/reporting_reader_password && test -r /run/reporting-secrets/reporting_reader_password'
docker exec --user 65534:65534 energy_reporting-reporting-1 sh -c \
  'test ! -r /run/reporting-secrets/reporting_reader_password && test ! -r /run/secrets/reporting_reader_password'
```

Also verify the database server process is non-root and Metabase/nginx have no
secret mounts. These probes require the successfully initialized container;
they do not initialize or repair a partially populated database.

The PostgreSQL bootstrap owner is an administrative account used only for
initialization/maintenance. Metabase must use `energy_reporting_reader`, which
gets CONNECT, schema USAGE and table SELECT only. PUBLIC database access is
revoked, including CREATE/TEMP privileges. No password is supplied by default.

## Validation and lifecycle commands

Use this shell function to fix the project, configuration file and environment
file explicitly. It prevents accidentally managing the retained demo project:

```bash
reporting_compose() {
  env -u REPORTING_HTTP_PORT -u REPORTING_RESTART_POLICY \
    -u REPORTING_OWNER_PASSWORD_FILE -u REPORTING_READER_PASSWORD_FILE \
    docker compose --project-name energy_reporting --env-file .env.reporting \
    --file docker-compose.yml "$@"
}

reporting_compose config --quiet
reporting_compose config --services
reporting_compose config --images
```

`config` checks interpolation and Compose structure, not secret-file contents or
successful initialization. Before `up`, check that both configured password paths
are regular, non-symlink, nonempty files owned by you with mode `0600`.
The wrapper makes `.env.reporting` the source of the reporting settings, avoiding
stale exported `REPORTING_*` values. Edit that file to change the port or policy.

Before resource creation, inspect existing Docker names and port bindings and
confirm `energy_reporting` has not already been used. The existing verified
dashboard may continue on port 3001 while this new project uses 3002. Changing
the new port to 3001 would conflict while that listener is running.

The verified command used locally available pinned images. On a clean Docker
installation, run `reporting_compose pull` first; registry availability and a
fresh-machine pull have not been verified by this test.

**First installation only, after confirming the project/resources are new:**

```bash
reporting_compose up -d --no-build --pull never --wait --wait-timeout 240
reporting_compose ps
curl --fail http://127.0.0.1:3002/api/health
```

For an existing, unchanged project, use the following non-deleting lifecycle
commands. `start` reuses containers; `up` can recreate them after configuration
changes. These commands are documented, not a claim that a stop/start cycle was
executed during fresh-install verification:

```bash
reporting_compose stop
reporting_compose start --wait --wait-timeout 240
reporting_compose ps
curl --fail http://127.0.0.1:3002/api/health
```

Keep port 3002 as the canonical default; this allows coexistence with an older
installation on port 3001 without adopting its storage or configuration.
Default restart policy remains `no`. Set `REPORTING_RESTART_POLICY=unless-stopped`
in `.env.reporting` only when unattended local startup is desired. Applying a
changed policy is a planned Compose deployment change, not an effect of editing
the file alone. Docker daemon restarts do not apply Compose dependency ordering
as a fresh `up` does. No existing policy needs to change for this guide to work.

PostgreSQL init scripts run only with an empty data volume. If initialization
fails, preserve the volume and logs for review; restarting does not rerun all
init scripts. Do not automatically reset volumes, use `down -v`, or replay init
scripts against an existing database as a repair. The disposable failed-install
reset below requires a deliberate data-loss decision and exact resource checks.

### Failed first initialization

The first attempt failed because UID 999 could not read a UID-1000 `0600` bind
mount. Its data directory already contained `PG_VERSION`; restarting would have
skipped initialization. A targeted empty-volume retry with the corrected tmpfs
bootstrap subsequently passed. The failed state was not repaired in place.

If initialization fails on your machine, preserve its state and inspect logs
privately. A nonempty volume must not be treated as a fresh installation.
Before choosing a reset, record exact container IDs and volume creation metadata,
confirm they belong only to that failed project, verify exclusive consumers and
establish that discarding the data is acceptable. Preserve any working reporting
installation and private credentials. Do not reset an established application.

The bounded reset should remove individually only the three failed-project
containers, then its PostgreSQL data volume and the verified unused Metabase
volume after checking zero references. Keep both project networks if unchanged.
Do not use force, prune or `down -v`. Revalidate the corrected Compose file,
secret paths, identities and free browser port before creating fresh volumes.
If a guard or operation fails, stop and preserve the partial state.

The retry must verify fresh PostgreSQL initialization, private tmpfs access,
reader SELECT and denied writes/CREATE/TEMP, the six-row/one-row sample, then
Metabase setup, all six questions and actual browser rendering. It must not be
reported as a complete ETL/dbt/Airflow fresh-install verification.

## Reporting data boundary

`docker/reporting/initdb/20-fixture.sql` creates a **static synthetic analytical
sample**: six monthly rows and one reconciliation row. Its values match the
analytical projection of the verified fixture snapshot; the header records the
tracked input fixture's SHA-256. It preserves one NULL, one zero and one negative
monthly observation, with display names left NULL.

This is a deliberately reduced reporting schema containing the columns used by
the six SQL questions. It does not copy historical ingestion UUIDs, claim new
audit provenance, reproduce every dbt mart column, or execute extraction,
validation, classification, warehouse publication or dbt. Original ETL and dbt
contracts remain in their existing files. See the [warehouse walkthrough](demo.md#isolated-warehouse-demonstration)
to verify those components separately.

The fixture sample is not synchronized automatically. Updating its two marts
requires an explicit matching column projection, independent source identity/value
checks and a separately approved reporting transaction. The separate real pilot
has a public manual refresh script described below; it does not modify this fixture
sample. Do not present the static sample as freshly executed ETL.

Expected monthly totals (MWh): June `331840.757`, July approximately
`4113664.903`, August `0`. Latest-month fuel share is NULL. July reconciliation
intentionally differs by `408774458.55` MWh; June/August totals are absent from
the comparison because the existing question uses an INNER JOIN.

Check your initialized database independently; this command reads only the
new reporting database through its container-local socket, without a password
argument:

```bash
reporting_compose exec -T reporting psql -X --no-password \
  --host /var/run/postgresql --port 5432 \
  --username energy_reporting_owner --dbname energy_reporting \
  --set ON_ERROR_STOP=1 <<'SQL'
BEGIN READ ONLY;
SELECT 'monthly' AS relation, COUNT(*) FROM marts.mart_monthly_generation
UNION ALL
SELECT 'reconciliation', COUNT(*) FROM marts.mart_reconciliation_total;
SELECT period, SUM(generation_mwh) AS total_generation_mwh
FROM marts.mart_monthly_generation GROUP BY period ORDER BY period;
SELECT has_table_privilege('energy_reporting_reader',
         'marts.mart_monthly_generation', 'SELECT') AS reader_select,
       has_table_privilege('energy_reporting_reader',
         'marts.mart_monthly_generation', 'INSERT') AS reader_insert,
       has_database_privilege('energy_reporting_reader',
         'energy_reporting', 'CREATE') AS reader_create,
       has_database_privilege('energy_reporting_reader',
         'energy_reporting', 'TEMP') AS reader_temp;
ROLLBACK;
SQL
```

Expected counts are `6` and `1`; the privilege row is `true, false, false, false`.
For exact individual values and keys, compare the two tables with the INSERT
rows in `20-fixture.sql`. Do not run the state-mutating warehouse integration
suite against this populated reporting database.

## Recreate the six-question dashboard

Fresh Metabase storage starts with no project questions or dashboard. Existing
application IDs and credentials are not imported. The tracked
`metabase/dashboards/energy_generation_dashboard.json` is a descriptive blueprint,
not an executable API import; automated dashboard provisioning is not implemented.

The verified run used Metabase's versioned setup/card/dashboard APIs as an
explicit provisioning step. No automatic dashboard installer or private
verification script is required by this public runbook. The equivalent manual
UI procedure below has not been independently rerun as a separate test:

1. Open `http://127.0.0.1:3002`; create a private administrator through the setup
   UI. Choose a strong private administrator password and retain it in a password
   manager. Do not put administrator credentials or session tokens in Git.
2. Add PostgreSQL connection: host `reporting`, port `5432`, database
   `energy_reporting`, user `energy_reporting_reader`. Enter the new reader
   password privately. Use the private backend connection; this local stack does
   not configure TLS for that connection. Never use the bootstrap owner.
3. Create six native SQL questions using the following files. Replace only the
   trusted identifier `{{ schema }}` with `marts` in the SQL editor; do not leave
   it as an unconfigured Metabase template variable. Create a collection named
   `Synthetic Reporting Fixture` and save all six questions there.

| File under `metabase/cards/` | Suggested name | Display / settings | Expected rows |
|---|---|---|---:|
| `01_total_generation_by_period.sql` | [Fixture] Monthly generation | Line; period → total_generation_mwh | 3 |
| `02_generation_by_state.sql` | [Fixture] Generation by state | Bar; location code → total_generation_mwh | 2 |
| `03_generation_by_fuel.sql` | [Fixture] Generation by fuel | Table; retain undefined/NULL share | 1 |
| `04_sector_breakdown.sql` | [Fixture] Generation by sector | Bar; sectorid code → total_generation_mwh | 2 |
| `05_reconciliation_check.sql` | [Fixture] Reconciliation check | Table; disclose incomplete fixture | 1 |
| `06_generation_trend.sql` | [Fixture] Sector trend | Line; period → total_generation_mwh, series sectorid | 4 |

4. Create `Energy Generation - Static Synthetic Reporting Fixture` in that
   collection and add all six questions. Use four charts and two tables; override
   the blueprint's proposed fuel pie with a table. Put monthly/state charts on
   the first row and fuel/sector on the second. Give reconciliation and trend
   separate full-width rows so the table columns remain readable. Display
   dimension codes rather than empty name fields; hide all-NULL name columns
   where appropriate. Add the incomplete-reconciliation explanation and label
   the dataset as a static synthetic reporting sample, not fresh ETL output.
5. Leave dashboard filters absent: parameter mappings are not implemented or
   verified. New card/dashboard IDs may differ; use the newly created dashboard
   URL, not a promise that its ID will be 2.
6. Verify each full result against independent reporting SQL, reader permissions,
   zero/negative/NULL behavior, unique business keys and actual browser rendering.
   Chart/table formatting may round displayed numbers; inspect full query results
   when comparing precision. The expected row counts above refer to query rows,
   not a promise that every row produces a connected line segment.

## Normal operation and older installations

Use port 3002 as the normal canonical reporting endpoint. Preserve its two data
volumes, the credential files and `.env.reporting`; Metabase's application volume
holds its administrator, database connection, saved questions and dashboard.
The public definitions describe how to recreate those objects, not an application
backup. Record the actual dashboard URL after setup; IDs may vary.

The verified new stack can serve the static reporting demonstration without an
older installation. It does not adopt that installation's volumes, credentials
or external nginx configuration. Equivalent query results do not establish
complete application-state or schema equivalence: an older snapshot can contain
extra provenance columns, and users, layouts, collections and connections can
differ. Review those differences and recovery coverage separately before retiring
anything. No automatic migration or retirement is part of this setup.

The canonical reporting stack subsequently passed a controlled stop/start
persistence check. Real warehouse-to-reporting manual refresh is described below.
Remaining reproduction work includes automated Metabase provisioning, fresh-machine
image/platform checks and fresh installation of the full ETL/dbt/Airflow environment.


## Real EIA reporting snapshot

A separate bounded real-data demonstration uses Texas, July 2026, Electric Utility
sector `1`. It retains 31 source/core records and 13 additive utility-scale rows,
reconciled to 11,036,928.08 MWh with a 0.00 MWh difference. This is one sector in
one state/month, not total Texas generation or a national publication.

The existing fixture dashboard, its questions and its `energy_reporting` database
remain separate. The real dashboard uses **a manually refreshed reporting copy**,
not a direct warehouse connection. The warehouse keeps TCP disabled and its
private Unix socket is not mounted into Metabase. No container recreation or new
Docker network/database listener is required.

The verified local connection is `energy_real_reporting_b43`, a new database in
the existing internal reporting PostgreSQL server. Its NOLOGIN owner is
`energy_real_owner_b43`. Metabase reuses the existing SELECT-only reporting login;
no password rotation or permissions change in the fixture database is needed.
The reader receives CONNECT, schema USAGE and SELECT in the new database only;
PUBLIC CREATE/TEMP access is revoked. The refresh operator uses local Docker
maintenance access, which is never given to Metabase.

The copy contains `marts.source_records` and `marts.snapshot_metadata`. Views expose
`marts.mart_monthly_generation` and `marts.pilot_reconciliation`. The source is the
verified dbt staging view and monthly mart, checked against successful live audit
records and their immutable raw pages. All 31 observations are retained; only
positively classified utility-scale leaves contribute to sums. Null, measured
zero and negative values remain distinct. The real pilot has no nulls; incomplete
leaf observations cannot be declared reconciled by replacing nulls with zero.

### Provisioning boundary and private configuration

The refresh script does **not** create databases, roles, schemas, containers or
services. Before using it, an operator must separately verify the reporting server
identity, create an unused `energy_real_reporting*` database with a dedicated
NOLOGIN `energy_real_owner*` owner, revoke PUBLIC database/CREATE/TEMP access, and
apply [reporting.sql](../metabase/real_data/reporting.sql) under that owner inside
an identity-guarded transaction. Grant the reporting login only CONNECT, schema
USAGE and SELECT. Record the actual database OID, cluster system identifier,
container ID and image digest. Never apply this schema to the fixture database.

Keep the source cluster, database OID, socket, role, raw roots and target identities
in an owner-only JSON file (`0600`, parent `0700`) outside Git. The following is a
**template**: replace the placeholder paths, zero OIDs and recorded identities
with independently verified values. It is intentionally not runnable as written.
No passwords or EIA key belong in this file.

```json
{
  "source": {
    "database": "energy_b42_pilot", "oid": 0, "user": "energy_b42_runner",
    "cluster": "energy_pg_test_b42_YYYYMMDD_01", "port": 55445,
    "socket": "/private/approved-pilot/postgres/socket",
    "dsn": "host=/private/approved-pilot/postgres/socket port=55445 dbname=energy_b42_pilot user=energy_b42_runner connect_timeout=5 passfile=/dev/null",
    "system_identifier": "RECORD_VERIFIED_IDENTIFIER",
    "data_directory": "/private/approved-pilot/postgres/pgdata",
    "control_binary": "/usr/lib/postgresql/18/bin/pg_controldata",
    "raw_roots": ["/private/approved-pilot/manual-raw", "/private/approved-pilot/airflow-raw"]
  },
  "target": {
    "database": "energy_real_reporting_pilot", "oid": 0,
    "owner": "energy_real_owner_pilot",
    "system_identifier": "RECORD_VERIFIED_IDENTIFIER",
    "container_id": "RECORD_FULL_CONTAINER_ID",
    "image": "RECORD_IMMUTABLE_IMAGE_DIGEST"
  },
  "scope": {"period": "2026-07", "location": "TX", "sectorid": "1", "source_rows": 31, "leaf_rows": 13},
  "output_directory": "/private/real-reporting/snapshots"
}
```

Create the private output directory with mode `0700` before running:

```bash
.venv/bin/python scripts/refresh_real_reporting.py \
  --config /private/real-reporting/refresh-config.json
```

The source connection uses a repeatable-read **READ ONLY** transaction. Identity,
positive classification, numeric conversion, business keys, raw page/batch hashes,
accepted provenance and dbt freshness must pass before a target write. The target
checks its database OID, cluster identity and owner, then replaces only the dedicated
reporting tables in one transaction. Failure rolls back; no reset, provisioning or
automatic retry occurs. Each attempted copy retains a new private JSON export.
Committed source and leaf projections are compared with the export afterward.
Run refreshes serially: no monotonic snapshot ordering is implemented between
concurrent operators. A successful reporting copy does not establish a source
warehouse revision; run the upstream pipeline and dbt separately first.
The Linux operator and Docker administrators remain trusted maintenance users.

### Metabase questions and refresh behavior

Register the new database on the existing internal `reporting:5432` endpoint using
the SELECT-only login. Use the [real dashboard blueprint](../metabase/dashboards/texas_real_eia_dashboard.json)
and its five SQL files. Each SQL statement uses fixed `marts` identifiers in the
separate database. Fuel codes provide labels where warehouse display names are
null. A signed bar chart and a zero/negative table preserve the actual observations;
there are no fabricated trends, nationwide rankings, pie shares or interactive filters.

The recorded local dashboard is **Texas Electricity Generation — July 2026 (Real
EIA Data)** at `http://127.0.0.1:3002/dashboard/3`, collection `4`, questions `33–37`,
connection `3`. These IDs identify this installation; new installations assign
independent IDs. Fixture dashboard `2` and questions `27–32` remain unchanged.

Run successful ingestion and dbt separately before an explicit copy refresh. Stale
materialized dbt data or inconsistent raw provenance causes rejection. Browser
reload and Metabase application/container startup only reread reporting storage;
they do not run ETL or copy warehouse data. Metabase query caching is a separate
layer: verification requests bypass it, and browser rereads must display the new
snapshot timestamp. No automatic reporting schedule is installed.

The freshness question distinguishes the historical July data period, local source
observation time (not an authoritative EIA revision timestamp), and copy-refresh
time, all explicitly labelled. The snapshot retains run IDs and content hashes.
The 122 unknown sector-95 observations and national reconciliation shortfall remain
outside this bounded pilot. Its successful reconciliation does not resolve them.
Original captures and operational/credential files remain private, outside the repository.
The README includes a [reviewed crop of the real dashboard](assets/texas-july-2026-eia-dashboard.png),
with interface navigation and the local ingestion-run identifier omitted. No data
values or chart content were changed.
