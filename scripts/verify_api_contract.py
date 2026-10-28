"""Report live API retrieval, contract and classification diagnostics.

Supply EIA_API_KEY securely in the process environment, then run:

    python scripts/verify_api_contract.py

This diagnostic never publishes data. Recognized facets alone do not establish
record validity or warehouse publication eligibility; UNKNOWN codes block it.

Three checks, in the order docs/source-data.md lists them as open items:

1. **Business-key uniqueness** -- the literal
       GROUP BY period, location, sectorid, fueltypeid HAVING COUNT(*) > 1
   check. The API has no GROUP BY, so it runs as a bounded, per-month
   paged scan: every row of a month is fetched and the key tuple is
   counted locally. One month costs ~1-4 requests at the 5,000-row JSON
   cap; ``--months N`` widens the scan to the N most recent months, one
   month at a time, so a rate limit mid-scan loses only the remaining
   months rather than the result.
2. **Live facet lists** -- the API's own ``location`` / ``sectorid`` /
   ``fueltypeid`` vocabularies, compared against the values derived from
   the bulk file and encoded in :mod:`energy_pipeline.records`.
3. **Pacific geography codes** -- which spellings the API actually
   carries. (The bulk file carries none: its state column holds
   only the 52 leaf geographies, so the API facet is the only
   source of the division vocabulary.)

The paged check reuses the production client and pagination, so it
exercises the same code path a real extraction does. Default cost is
~3-6 requests, which fits inside DEMO_KEY's measured 10-request budget;
a registered key is required for anything wider. The scan stops before
the rate budget is exhausted rather than sleeping out a ~96 minute
lockout (measured ``Retry-After: 5793``).
"""

from __future__ import annotations

import argparse
import collections
import sys
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# The imports below the sys.path bootstrap come after it, or importing the
# package fails.
from energy_pipeline.client import USER_AGENT, EiaClient
from energy_pipeline.config import Settings
from energy_pipeline.extract import shift_month
from energy_pipeline.pagination import iter_pages
from energy_pipeline.records import (
    BASE_ENERGY_SOURCES,
    KNOWN_ENERGY_SOURCE_AGGREGATES,
    LEAF,
    ROLLUP,
    UNKNOWN,
    classify_energy_source,
    classify_geography,
    classify_sector,
)

KEY_FIELDS = ("period", "location", "sectorid", "fueltypeid")
FACET_NAMES = ("location", "sectorid", "fueltypeid")
#: Every Pacific-division spelling seen on either side of the source:
#: the API facet read carried PCC/PCN/PAC, and PACC/PACN were once
#: (wrongly) attributed to the bulk file.
PACIFIC_SPELLINGS = ("PACC", "PACN", "PCC", "PCN", "PAC")


def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return payload.get("response", {}).get("data") or []


def latest_period(client: EiaClient, settings: Settings) -> str:
    payload = client.get(
        {
            "frequency": settings.frequency,
            "data[0]": settings.measure,
            "length": 1,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
        }
    )
    rows = _rows(payload)
    if not rows:
        raise SystemExit("the API returned no rows for the latest-period probe")
    return str(rows[0]["period"])


def scan_month(client: EiaClient, settings: Settings, month: str) -> dict[str, Any]:
    """Page one month in full and count candidate business keys."""
    keys: collections.Counter = collections.Counter()
    pages = 0
    total: int | None = None

    def fetch(offset: int, length: int) -> dict[str, Any]:
        return client.get(
            {
                "frequency": settings.frequency,
                "data[0]": settings.measure,
                "start": month,
                "end": month,
                "offset": offset,
                "length": length,
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
            }
        )

    for page in iter_pages(fetch, page_size=settings.page_size):
        pages += 1
        total = page.total
        for row in page.rows:
            keys[tuple(str(row[field]) for field in KEY_FIELDS)] += 1
    return {
        "month": month,
        "pages": pages,
        "source_total": total,
        "rows": sum(keys.values()),
        "distinct_keys": len(keys),
        "duplicates": {key: n for key, n in keys.items() if n > 1},
    }


def fetch_facet_members(settings: Settings, name: str) -> tuple[set[str], dict[str, str]]:
    """Fetch the live member set for one facet via the route's ``/facet/{name}`` endpoint.

    Returns ``(ids, names)`` where ``names`` maps each id to its human-readable label.

    The EIA API v2 exposes facet vocabularies per-field at
    ``{base_url}/{route}/facet/{name}``, returning ``response.facets`` as a list
    of ``{"id": ..., "name": ...}``. A plain request, not through EiaClient:
    the client is bound to the data endpoint, and a single facet call needs no
    retry machinery -- if it fails, the operator re-runs it.
    """
    try:
        response = requests.get(
            f"{settings.base_url}/{settings.route}/facet/{name}",
            params={"api_key": settings.api_key},
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=settings.timeout_seconds,
        )
    except requests.RequestException as exc:
        # Request exceptions may contain the authenticated URL. Report only type.
        raise SystemExit(f"facet {name} retrieval: FAIL ({type(exc).__name__})") from None
    if response.status_code >= 400:
        retry_after = response.headers.get("Retry-After")
        raise SystemExit(
            f"facet {name} retrieval: FAIL (HTTP {response.status_code})"
            + (f", Retry-After: {retry_after}s" if retry_after else "")
        )
    print(f"facet {name} retrieval: PASS (HTTP {response.status_code})")
    try:
        facets = response.json()["response"]["facets"]
    except (ValueError, KeyError, TypeError):
        raise SystemExit(f"facet schema/contract: FAIL ({name}: invalid envelope)") from None
    if not isinstance(facets, list) or any(
        not isinstance(item, dict) or item.get("id") in (None, "") for item in facets
    ):
        raise SystemExit(f"facet schema/contract: FAIL ({name}: invalid member list)")
    ids = {str(item["id"]) for item in facets}
    names = {str(item["id"]): item.get("name", "") for item in facets}
    return ids, names


def facet_members(payload: dict[str, Any], name: str) -> set[str]:
    """Extract members from a facets envelope.

    Accepts two shapes the API envelope may take:
    - ``facets[name]`` as a dict of ``{value: {...}}``
    - ``facets[name]`` as a list of ``{"id": ..., "name": ...}``
    """
    node = payload.get("facets", {}).get(name)
    if isinstance(node, dict):
        return {str(value) for value in node}
    if isinstance(node, list):
        members: set[str] = set()
        for item in node:
            if isinstance(item, dict):
                members.add(
                    str(item.get("id") or item.get("value") or item.get("code") or item.get("name"))
                )
            else:
                members.add(str(item))
        return members
    return set()


def report_uniqueness(client: EiaClient, settings: Settings, months: int) -> bool:
    latest = latest_period(client, settings)
    print(f"latest period on the route : {latest}")
    print(
        f"rate limit                 : {client.rate_limit} requests, "
        f"{client.rate_remaining} remaining\n"
    )
    window = [shift_month(latest, -(n - 1)) for n in range(months, 0, -1)]
    ok = True
    for month in window:
        if client.rate_remaining is not None and client.rate_remaining < 2 and month != window[0]:
            print(f"  stopping before {month}: rate budget nearly exhausted")
            print("  uniqueness is verified only for the months above")
            return ok
        result = scan_month(client, settings, month)
        if result["source_total"] is None:
            coverage = "complete (source gave no total; ended on a short page)"
        elif result["rows"] == result["source_total"]:
            coverage = "complete"
        else:
            coverage = "PARTIAL"
        total = f"{result['source_total']:,}" if result["source_total"] is not None else "no total"
        print(
            f"  {month}: {result['pages']} page(s), {result['rows']:,} rows "
            f"of {total} ({coverage}), "
            f"{result['distinct_keys']:,} distinct keys"
        )
        if result["duplicates"]:
            ok = False
            for key, count in sorted(result["duplicates"].items()):
                print(f"    DUPLICATE x{count}: {key}")
        else:
            print("    no duplicate keys")
    return ok


def report_facets(settings: Settings) -> bool:
    """Return diagnostic success, never authorization to publish warehouse rows."""
    # fetch_facet_members returns (ids, names)
    facet_data = {name: fetch_facet_members(settings, name) for name in FACET_NAMES}
    members = {name: facet_data[name][0] for name in FACET_NAMES}
    names = {name: facet_data[name][1] for name in FACET_NAMES}
    for name in FACET_NAMES:
        print(f"  {name}: {len(members[name])} members")

    missing = [name for name in FACET_NAMES if not members[name]]
    if missing:
        # An empty member set is not a match -- it means the endpoint
        # returned something other than the expected facet envelope.
        print(f"facet schema/contract: FAIL (no members returned for: {missing})")
        print("classification: NOT CHECKED (incomplete facet vocabulary)")
        print("warehouse publication eligibility: NOT VERIFIED")
        return False
    print("facet schema/contract: PASS (nonempty member lists; row schema not checked)")
    classifiers = {
        "location": classify_geography,
        "sectorid": classify_sector,
        "fueltypeid": classify_energy_source,
    }
    unknown_codes = {
        name: sorted(code for code in members[name] if classifiers[name](code) == UNKNOWN)
        for name in FACET_NAMES
    }
    api_sectors = members["sectorid"]
    extra_sectors = unknown_codes["sectorid"]
    if extra_sectors:
        print("    !! API sectors outside the bulk-derived 1-7 (classified UNKNOWN):")
        for sid in extra_sectors:
            print(f"       {sid}: {names['sectorid'][sid]}")
    else:
        print(f"    observed sectors belong to the classified 1-7 seed: {sorted(api_sectors)}")

    api_fuels = members["fueltypeid"]
    base = sorted(api_fuels & BASE_ENERGY_SOURCES)
    aggregates = sorted(api_fuels & KNOWN_ENERGY_SOURCE_AGGREGATES)
    unknown = unknown_codes["fueltypeid"]
    print(f"    base fuel codes ({len(base)}): {base}")
    print(f"    aggregate codes ({len(aggregates)}): {aggregates}")
    if unknown:
        print(f"    !! {len(unknown)} codes unknown (classified UNKNOWN):")
        for fid in unknown:
            print(f"       {fid}: {names['fueltypeid'][fid]}")
        print("       these would classify as 'unknown' -- accepted at ingestion,")
        print("       blocked at aggregate publish pending evidence-based classification review.")
    else:
        print("    every API fuel code is known to the bulk-derived vocabulary")

    api_locations = members["location"]
    pacific = sorted(api_locations.intersection(PACIFIC_SPELLINGS))
    print(f"    Pacific-division spellings the API carries: {pacific}")
    for pc in pacific:
        print(f"       {pc}: {names['location'][pc]}")
    print(
        "    the bulk file carries no division codes at all (measured); "
        "the classifier retains historically reported API and legacy Pacific spellings"
    )
    for classification in (LEAF, ROLLUP):
        codes = sorted(code for code in api_locations if classify_geography(code) == classification)
        print(f"    geography {classification} ({len(codes)}): {codes}")
    for name, codes in unknown_codes.items():
        if codes:
            print(f"    {name} UNKNOWN: {codes}")
    if any(unknown_codes.values()):
        print("classification: FAIL (UNKNOWN dimension codes)")
        print("warehouse publication eligibility: BLOCKED (UNKNOWN codes in returned vocabulary)")
        print("    A filtered sample still requires record validation and aggregate_publish_gate.")
        return False
    print("classification: PASS (returned facet codes recognized)")
    print("warehouse publication eligibility: NOT VERIFIED (no records validated or published)")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify the live API contract: key uniqueness, facets, Pacific codes."
    )
    parser.add_argument(
        "--months",
        type=int,
        default=1,
        help="scan the N most recent months (default 1; each costs ~1-4 requests)",
    )
    parser.add_argument("--skip-uniqueness", action="store_true")
    parser.add_argument("--skip-facets", action="store_true")
    args = parser.parse_args()

    settings = Settings.from_env()
    if not settings.api_key:
        raise SystemExit("EIA_API_KEY is not set; the client refuses unauthenticated requests")

    client = EiaClient(settings)
    ok = True

    if not args.skip_uniqueness:
        print("=== 1. Business-key uniqueness (the §6a check, per month) ===")
        ok = report_uniqueness(client, settings, args.months) and ok
        print(
            f"\n  rate limit after scan   : {client.rate_limit} requests, "
            f"{client.rate_remaining} remaining\n"
        )

    if not args.skip_facets:
        print("=== 2./3. Live facet lists and Pacific geography codes ===")
        ok = report_facets(settings) and ok
    else:
        print("facet retrieval and schema/contract: NOT CHECKED (--skip-facets)")
        print("classification: NOT CHECKED (--skip-facets)")
        print("warehouse publication eligibility: NOT VERIFIED")

    print("\nDIAGNOSTIC VERDICT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
