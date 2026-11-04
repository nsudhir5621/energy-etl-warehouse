#!/usr/bin/env bash
# Stage only the new reader secret before the stock image drops to postgres.
set -euo pipefail

reporting_source=/run/secrets/reporting_reader_password
reporting_runtime=/run/reporting-secrets

if [[ "$EUID" != 0 || "${1:-}" != postgres ]]; then
    printf '%s\n' 'Reporting bootstrap requires the stock root-to-postgres startup path.' >&2
    exit 1
fi
if [[ ! -f "$reporting_source" || -L "$reporting_source" || ! -s "$reporting_source" \
    || "$(stat -c '%a' "$reporting_source")" != 600 ]]; then
    printf '%s\n' 'Reporting reader secret must be a nonempty regular file with mode 0600.' >&2
    exit 1
fi
if [[ ! -d "$reporting_runtime" || -L "$reporting_runtime" \
    || "$(stat -f -c '%T' "$reporting_runtime")" != tmpfs ]]; then
    printf '%s\n' 'Reporting secret staging requires its dedicated tmpfs mount.' >&2
    exit 1
fi

# Use the image account by name; do not require a particular host/container UID.
chown postgres:postgres "$reporting_runtime"
chmod 0700 "$reporting_runtime"
install --owner=postgres --group=postgres --mode=0600 --no-target-directory \
    "$reporting_source" "$reporting_runtime/reporting_reader_password"

# The unmodified official entrypoint performs initialization and drops privileges.
exec /usr/local/bin/docker-entrypoint.sh "$@"
