#!/usr/bin/env python3
"""Explicit manual refresh of a separate, already provisioned reporting database.

Reads verified dbt outputs and immutable raw provenance in a READ ONLY source
transaction. Uses docker exec/psql stdin to replace only dedicated reporting
tables atomically. No API key, passwords, defaults, provisioning, ETL or services.
See docs/reporting.md for the private configuration and deployment boundary.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import re
import stat
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from energy_pipeline.records import (
    LEAF,
    ROLLUP,
    UNKNOWN,
    classify_energy_source,
    classify_geography,
    classify_sector,
    parse_record,
    validate_generation_contract,
)

MONTHLY_COLUMNS = [
    "period",
    "location",
    "sectorid",
    "fueltypeid",
    "generation_thousand_mwh",
    "generation_mwh",
    "is_rollup_geography",
    "is_rollup_sector",
    "is_rollup_energy_source",
    "run_id",
    "content_hash",
    "location_name",
    "sector_name",
    "energy_source_name",
    "year",
    "month",
    "date_month",
]
COLUMNS = [*MONTHLY_COLUMNS, "is_leaf"]


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def validate_target(target):
    if not re.fullmatch(r"energy_real_reporting[a-z0-9_]*", target["database"]):
        raise ValueError("A dedicated real-reporting database is required")
    if not re.fullmatch(r"energy_real_owner[a-z0-9_]*", target["owner"]):
        raise ValueError("A dedicated reporting owner is required")
    if type(target["oid"]) is not int or target["oid"] <= 0:
        raise ValueError("Explicit reporting database OID required")
    if not re.fullmatch(r"[0-9]{16,20}", target["system_identifier"]):
        raise ValueError("Explicit reporting cluster identity required")


def load_config(path):
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or path.is_symlink()
        or stat.S_IMODE(path.parent.stat().st_mode) != 0o700
    ):
        raise ValueError("Configuration must be private, regular and owner-only")
    config = json.loads(path.read_text())
    source, target = config["source"], config["target"]
    validate_target(target)
    scope = config["scope"]
    if (
        scope["period"],
        scope["location"],
        scope["sectorid"],
        scope["source_rows"],
        scope["leaf_rows"],
    ) != ("2026-07", "TX", "1", 31, 13):
        raise ValueError("This dashboard requires the approved Texas July sector-1 scope")
    control = Path(source["control_binary"])
    data = Path(source["data_directory"])
    if (
        control.name != "pg_controldata"
        or not control.is_file()
        or control.stat().st_uid != 0
        or stat.S_IMODE(control.stat().st_mode) & 0o022
        or data.resolve() != data
        or not data.is_dir()
        or data.stat().st_uid != os.getuid()
        or stat.S_IMODE(data.stat().st_mode) != 0o700
    ):
        raise ValueError("Unsafe source cluster/control paths")
    for identifier in (source["system_identifier"], target["system_identifier"]):
        if not re.fullmatch(r"[0-9]{16,20}", identifier):
            raise ValueError("Explicit numeric cluster system identifiers required")
    for field in (source["database"], source["user"], source["cluster"], target["owner"]):
        if not re.fullmatch(r"[a-z][a-z0-9_]+", field):
            raise ValueError("Invalid explicit identity")
    if source["database"] in {"energy", "postgres", "energy_reporting"}:
        raise ValueError("Protected source database")
    for value in (source["oid"], target["oid"]):
        if type(value) is not int or value <= 0:
            raise ValueError("Explicit database OIDs required")
    socket = Path(source["socket"])
    if not socket.is_absolute() or socket.resolve() != socket or not socket.is_dir():
        raise ValueError("Explicit private source socket required")
    if socket.stat().st_uid != os.getuid() or stat.S_IMODE(socket.stat().st_mode) != 0o700:
        raise ValueError("Unsafe source socket directory")
    if (
        type(source["port"]) is not int
        or not 1024 <= source["port"] <= 65535
        or source["port"] == 5432
    ):
        raise ValueError("Explicit nondefault source port required")
    canonical = (
        f"host={socket} port={source['port']} dbname={source['database']} "
        f"user={source['user']} connect_timeout=5 passfile=/dev/null"
    )
    if source["dsn"] != canonical:
        raise ValueError("Source DSN differs from explicit approved identity")
    if not re.fullmatch(r"[a-f0-9]{64}", target["container_id"]):
        raise ValueError("Exact reporting container ID required")
    return config


def validate_snapshot(staging, monthly, scope):
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", scope["period"]):
        raise ValueError("Invalid monthly scope")
    expected = {}
    for row in staging:
        if any(row[k] != scope[k] for k in ("period", "location", "sectorid")):
            raise ValueError("Row outside declared scope")
        generation = validate_generation_contract(
            row["generation_thousand_mwh"], "thousand megawatthours"
        )
        classes = (
            classify_geography(row["location"]),
            classify_sector(row["sectorid"]),
            classify_energy_source(row["fueltypeid"]),
        )
        if UNKNOWN in classes or classes[0] != LEAF or row["sectorid"] not in "1234567":
            raise ValueError("Unclassified or non-utility scope")
        flags = [
            row[k] for k in ("is_rollup_geography", "is_rollup_sector", "is_rollup_energy_source")
        ]
        if flags != [c == ROLLUP for c in classes] or row["is_leaf"] != all(
            c == LEAF for c in classes
        ):
            raise ValueError("Classification flags disagree with positive membership")
        if row["fueltypeid"] in expected:
            raise ValueError("Duplicate source business key")
        expected[row["fueltypeid"]] = row
        value = row["generation_mwh"]
        if generation is None:
            if value is not None:
                raise ValueError("NULL generation changed")
        elif (
            value is None
            or not math.isfinite(value)
            or not math.isclose(value, generation * 1000, rel_tol=0, abs_tol=0.000001)
        ):
            raise ValueError("Incorrect numeric conversion")
    leaves = {k: v for k, v in expected.items() if v["is_leaf"] and k != "DPV"}
    if len(monthly) != len(leaves) or len({r["fueltypeid"] for r in monthly}) != len(monthly):
        raise ValueError("dbt mart key set differs from classified source")
    for row in monthly:
        source = leaves.get(row["fueltypeid"])
        if source is None or any(row[k] != source[k] for k in MONTHLY_COLUMNS):
            raise ValueError("dbt mart values or provenance are stale")
        if row["generation_mwh"] is None:
            raise ValueError("NULL leaf prevents complete bounded reconciliation")
    total = expected.get("ALL", {}).get("generation_mwh")
    if (
        total is None
        or not monthly
        or abs(math.fsum(r["generation_mwh"] for r in monthly) - total) > 0.01
    ):
        raise ValueError("Bounded source-to-mart reconciliation failed")
    return total


def raw_records(source, audit):
    observed = datetime.fromisoformat(audit["extracted_at"]).date().isoformat()
    key = (
        f"raw/eia/electric_power_operational_data/ingestion_date={observed}/"
        f"run_id={uuid.UUID(audit['run_id'])}/manifest.json"
    )
    candidates = [Path(root) / key for root in source["raw_roots"] if (Path(root) / key).is_file()]
    if len(candidates) != 1:
        raise ValueError("Exactly one immutable raw manifest required")
    path = candidates[0]
    if path.resolve() != path:
        raise ValueError("Unexpected raw symlink")
    manifest = json.loads(path.read_text())
    if (
        manifest["run_id"] != audit["run_id"]
        or manifest["content_hash"] != audit["content_hash"]
        or manifest["source"] != "eia_api_v2_live"
        or manifest["frequency"] != "monthly"
        or manifest["measure"] != "generation"
        or manifest["route"] != "electricity/electric-power-operational-data"
    ):
        raise ValueError("Raw provenance mismatch")
    scope = json.loads((path.parent / "source_scope.json").read_text())
    if scope["run_id"] != audit["run_id"] or scope["content_hash"] != audit["content_hash"]:
        raise ValueError("Raw scope receipt mismatch")
    root = next(Path(r) for r in source["raw_roots"] if path == Path(r) / key)
    content = hashlib.sha256()
    rows = []
    for obj in manifest["objects"]:
        page = root / obj["path"]
        if not page.is_relative_to(path.parent) or page.resolve() != page:
            raise ValueError("Unsafe raw object path")
        data = page.read_bytes()
        if hashlib.sha256(data).hexdigest() != obj["sha256"]:
            raise ValueError("Raw page checksum mismatch")
        content.update(obj["sha256"].encode("ascii"))
        rows.extend(json.loads(data))
    if content.hexdigest() != audit["content_hash"] or len(rows) != manifest["record_count"]:
        raise ValueError("Raw batch content mismatch")
    parsed = [parse_record(r) for r in rows]
    if len({r.key for r in parsed}) != len(parsed):
        raise ValueError("Duplicate raw business keys")
    return {r.key: r for r in parsed}


def source_snapshot(config):
    source, scope = config["source"], config["scope"]
    control = subprocess.run(
        [source["control_binary"], source["data_directory"]],
        capture_output=True,
        text=True,
        check=True,
    )
    if not re.search(
        r"Database system identifier:\s+" + source["system_identifier"] + r"\b", control.stdout
    ):
        raise ValueError("Source physical cluster identity mismatch")
    with psycopg.connect(source["dsn"], autocommit=True) as conn, conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        identity = conn.execute(
            "SELECT current_database(),current_user,current_setting('cluster_name'),"
            "current_setting('port'),inet_server_addr(),d.oid,"
            "r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls "
            "FROM pg_database d JOIN pg_roles r ON r.rolname=current_user "
            "WHERE d.datname=current_database()"
        ).fetchone()
        if identity != (
            source["database"],
            source["user"],
            source["cluster"],
            str(source["port"]),
            None,
            source["oid"],
            False,
        ):
            raise ValueError("Source connection identity mismatch")
        params = (scope["period"], scope["location"], scope["sectorid"])

        def rows(table):
            return conn.execute(
                "SELECT row_to_json(t) FROM marts." + table + " t WHERE period=%s AND location=%s "
                "AND sectorid=%s ORDER BY fueltypeid LIMIT 1001",
                params,
            ).fetchall()

        staging = [r[0] for r in rows("stg_generation")]
        monthly = [r[0] for r in rows("mart_monthly_generation")]
        if len(staging) != scope["source_rows"] or len(monthly) != scope["leaf_rows"]:
            raise ValueError("Source cardinality differs from approved scope")
        total = validate_snapshot(staging, monthly, scope)
        runs = sorted({r["run_id"] for r in staging})
        audit = [
            r[0]
            for r in conn.execute(
                "SELECT json_build_object('run_id',run_id::text,'content_hash',content_hash,"
                "'source',source,'status',status,'extracted_at',extracted_at) "
                "FROM audit.ingestion_run "
                "WHERE run_id::text=ANY(%s) ORDER BY extracted_at",
                (runs,),
            ).fetchall()
        ]
        if len(audit) != len(runs) or any(
            a["source"] != "eia_api_v2_live" or a["status"] != "succeeded" for a in audit
        ):
            raise ValueError("Accepted live provenance required")
        raw = {a["run_id"]: raw_records(source, a) for a in audit}
        for row in staging:
            record = raw[row["run_id"]].get(
                tuple(row[k] for k in ("period", "location", "sectorid", "fueltypeid"))
            )
            if record is None or record.generation != row["generation_thousand_mwh"]:
                raise ValueError("Raw-to-warehouse observation mismatch")
        all_row = next(r for r in staging if r["fueltypeid"] == "ALL")
        raw_total = (
            raw[all_row["run_id"]][
                (scope["period"], scope["location"], scope["sectorid"], "ALL")
            ].generation
            * 1000
        )
        if abs(raw_total - total) > 0.01:
            raise ValueError("Raw source ALL differs from warehouse")
    metadata = {
        "singleton": True,
        "snapshot_id": str(uuid.uuid4()),
        "refreshed_at": datetime.now(UTC).isoformat(),
        "source_observed_min": audit[0]["extracted_at"],
        "source_observed_max": audit[-1]["extracted_at"],
        **{k: scope[k] for k in ("period", "location", "sectorid")},
        "source_total_mwh": raw_total,
        "source_rows": len(staging),
        "leaf_rows": len(monthly),
        "provenance": audit,
    }
    return {
        "source_records": [{k: r[k] for k in COLUMNS} for r in staging],
        "snapshot_metadata": [metadata],
        "expected_monthly": monthly,
    }


def target_command(target):
    validate_target(target)
    info = json.loads(subprocess.check_output(["docker", "inspect", target["container_id"]]))[0]
    if (
        info["Id"] != target["container_id"]
        or info["Image"] != target["image"]
        or not info["State"]["Running"]
        or info["HostConfig"]["PortBindings"]
    ):
        raise ValueError("Reporting container identity or isolation mismatch")
    return [
        "docker",
        "exec",
        "-i",
        "--user",
        "postgres",
        target["container_id"],
        "psql",
        "-X",
        "-A",
        "-t",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "energy_reporting_owner",
        "-d",
        target["database"],
    ]


def refresh_sql(target, payload):
    validate_target(target)
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    # Target identifiers have been whitelisted; data travels only as base64 JSON.
    return f"""BEGIN;
SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='30s';
DO $guard$ BEGIN
 IF current_database()<>'{target["database"]}'
 OR (SELECT oid FROM pg_database WHERE datname=current_database())<>{target["oid"]}
 OR (SELECT system_identifier::text FROM pg_control_system())<>'{target["system_identifier"]}'
 OR (SELECT pg_get_userbyid(datdba) FROM pg_database
     WHERE datname=current_database())<>'{target["owner"]}'
 THEN RAISE EXCEPTION 'Reporting identity mismatch'; END IF;
END $guard$;
SET LOCAL ROLE "{target["owner"]}";
LOCK TABLE marts.source_records,marts.snapshot_metadata IN SHARE ROW EXCLUSIVE MODE;
DELETE FROM marts.source_records; DELETE FROM marts.snapshot_metadata;
INSERT INTO marts.source_records
 SELECT x.* FROM jsonb_populate_recordset(NULL::marts.source_records,
 convert_from(decode('{encoded}','base64'),'UTF8')::jsonb->'source_records') x;
INSERT INTO marts.snapshot_metadata
 SELECT x.* FROM jsonb_populate_recordset(NULL::marts.snapshot_metadata,
 convert_from(decode('{encoded}','base64'),'UTF8')::jsonb->'snapshot_metadata') x;
DO $check$ BEGIN
 IF (SELECT count(*) FROM marts.source_records)<>{len(payload["source_records"])}
 OR (SELECT count(*) FROM marts.mart_monthly_generation)<>{len(payload["expected_monthly"])}
 OR (SELECT abs(gap_mwh)>.01 FROM marts.pilot_reconciliation)
 THEN RAISE EXCEPTION 'Snapshot validation failed'; END IF;
END $check$;
COMMIT;
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    config = load_config(args.config)
    payload = source_snapshot(config)
    output = Path(config["output_directory"])
    if (
        not output.is_dir()
        or output.resolve() != output
        or output.stat().st_uid != os.getuid()
        or stat.S_IMODE(output.stat().st_mode) != 0o700
    ):
        raise ValueError("Private existing snapshot output directory required")
    identifier = payload["snapshot_metadata"][0]["snapshot_id"]
    path = output / f"snapshot-{identifier}.json"
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(payload, stream, indent=2)
    command = target_command(config["target"])
    result = subprocess.run(
        command,
        input=refresh_sql(config["target"], payload),
        capture_output=True,
        text=True,
        timeout=45,
    )
    if result.returncode:
        raise RuntimeError("Reporting refresh rolled back; export preserved, no automatic retry")
    query = (
        "SELECT json_build_object('source_records',"
        "(SELECT json_agg(row_to_json(t) ORDER BY fueltypeid) FROM marts.source_records t),"
        "'monthly',(SELECT json_agg(row_to_json(t) ORDER BY fueltypeid) "
        "FROM marts.mart_monthly_generation t));"
    )
    check = subprocess.run(
        command, input=query, capture_output=True, text=True, timeout=20, check=True
    )
    actual = json.loads(check.stdout)
    if digest(actual["source_records"]) != digest(payload["source_records"]) or digest(
        actual["monthly"]
    ) != digest(payload["expected_monthly"]):
        raise ValueError("Committed reporting snapshot differs from dbt export; preserve evidence")
    print(
        json.dumps(
            {
                "snapshot_id": identifier,
                "source_rows": len(payload["source_records"]),
                "leaf_rows": len(payload["expected_monthly"]),
                "source_total_mwh": payload["snapshot_metadata"][0]["source_total_mwh"],
                "source_records_sha256": digest(payload["source_records"]),
                "monthly_sha256": digest(payload["expected_monthly"]),
                "reporting_copy_matches_source": True,
                "export_file": str(path),
            }
        )
    )


if __name__ == "__main__":
    main()
