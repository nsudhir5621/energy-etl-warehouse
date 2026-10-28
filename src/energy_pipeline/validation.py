"""Source-response validation, with an explicit severity for every finding.

The severity split is a design decision, not a mood:

**HARD** — a finding that must stop the run, because continuing would publish
data we cannot defend. Row contract violations, and duplicate keys whose
payloads conflict (the source contradicted itself and we have no principled way
to pick a winner).

**WARN** — a finding that must be *visible* but must not stop the run, because
failing on it would be worse than continuing. A dimension code the current seed
does not classify is the clear case: EIA adding a census division is a routine
event, and a pipeline that halts on it is a pipeline that will be switched off.
The row is still ingested — refusing the raw data over a missing dimension
member is a worse failure than carrying it labelled.

**INFO** — measured characteristics of the batch, carried forward as evidence
rather than as complaints. Rollup counts, null counts and negative counts are
observations, and publishing them is what lets a later assumption be checked
against reality instead of re-argued.

The WARN/accept stance above is scoped to *ingestion*. An unclassified code is
still barred from an aggregate — see :func:`aggregate_publish_gate`, which is
where the risk actually lives.
"""

from __future__ import annotations

import enum
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from .errors import ValidationError
from .records import (
    DIMENSIONS,
    DedupeResult,
    GenerationRecord,
    unknown_codes,
    validate_generation_contract,
)


class Severity(enum.Enum):
    HARD = "hard"
    WARN = "warn"
    INFO = "info"


@dataclass(frozen=True, slots=True)
class Finding:
    severity: Severity
    check: str
    message: str
    count: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity.value,
            "check": self.check,
            "message": self.message,
            "count": self.count,
        }


@dataclass
class ValidationReport:
    findings: list[Finding] = field(default_factory=list)
    record_count: int = 0
    unique_key_count: int = 0
    null_generation_count: int = 0
    zero_generation_count: int = 0
    negative_generation_count: int = 0
    #: Rows containing at least one known rollup dimension; may overlap unknowns.
    rollup_row_count: int = 0
    #: Rows whose dimensions are all positively classified as leaves.
    leaf_row_count: int = 0
    #: Rows containing unknown dimensions, counted once per record. May overlap
    #: rollup_row_count; the three counters need not sum to record_count.
    unknown_row_count: int = 0
    #: Dimension name -> set of unclassified codes seen.
    unknown_codes: dict[str, set[str]] = field(default_factory=dict)

    @property
    def hard_failures(self) -> list[Finding]:
        return [f for f in self.findings if f.severity is Severity.HARD]

    @property
    def ok(self) -> bool:
        """Whether the batch may proceed. Warnings never block."""
        return not self.hard_failures

    def add(self, severity: Severity, check: str, message: str, count: int = 1) -> None:
        self.findings.append(Finding(severity, check, message, count))

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "record_count": self.record_count,
            "unique_key_count": self.unique_key_count,
            "null_generation_count": self.null_generation_count,
            "zero_generation_count": self.zero_generation_count,
            "negative_generation_count": self.negative_generation_count,
            "rollup_row_count": self.rollup_row_count,
            "leaf_row_count": self.leaf_row_count,
            "unknown_row_count": self.unknown_row_count,
            "unknown_codes": {k: sorted(v) for k, v in sorted(self.unknown_codes.items())},
            "findings": [f.to_dict() for f in self.findings],
        }


def validate(
    records: Sequence[GenerationRecord],
    *,
    parse_errors: Sequence[str] = (),
    dedupe_result: DedupeResult | None = None,
    source_record_total: int | None = None,
    window_start: str = "",
    window_end: str = "",
) -> ValidationReport:
    """Validate one batch after parsing and deduplication."""
    report = ValidationReport(
        record_count=len(records), unique_key_count=len({r.key for r in records})
    )

    for error in parse_errors:
        report.add(Severity.HARD, "row_contract", error)

    for record in records:
        try:
            generation = validate_generation_contract(record.generation, record.generation_units)
        except ValidationError as exc:
            report.add(Severity.HARD, "row_contract", str(exc))
        else:
            if generation is None:
                report.null_generation_count += 1
            elif generation == 0:
                report.zero_generation_count += 1
            elif generation < 0:
                report.negative_generation_count += 1

        if record.is_rollup:
            report.rollup_row_count += 1
        elif record.is_leaf:
            report.leaf_row_count += 1

        unclassified = unknown_codes(record)
        if unclassified:
            report.unknown_row_count += 1
            for dimension, code in unclassified.items():
                report.unknown_codes.setdefault(dimension, set()).add(code)

    _check_key_uniqueness(report, records, dedupe_result)
    _check_recognised_codes(report)
    _check_completeness(report, source_record_total, window_start, window_end)
    _add_observations(report)

    return report


def _check_key_uniqueness(
    report: ValidationReport,
    records: Sequence[GenerationRecord],
    dedupe_result: DedupeResult | None,
) -> None:
    """The batch-level half of the uniqueness contract.

    ``GROUP BY period, location, sectorid, fueltypeid HAVING COUNT(*) > 1``. The
    offline half of the Phase 0 verification: it proves the *pipeline* never
    hands MERGE two rows with the same key. The online half — that the *source*
    never returns them — is a separate live gate, recorded in docs/source-data.md.
    """
    seen: dict[tuple[str, str, str, str], GenerationRecord] = {}
    for record in records:
        seen.setdefault(record.key, record)
    report.unique_key_count = len(seen)

    if len(seen) != len(records):
        report.add(
            Severity.HARD,
            "key_uniqueness",
            f"{len(records) - len(seen)} duplicate key(s) survived batch-level deduplication",
            count=len(records) - len(seen),
        )

    if dedupe_result is not None:
        if dedupe_result.identical_duplicates:
            report.add(
                Severity.INFO,
                "duplicate_collapse",
                f"{dedupe_result.identical_duplicates} identical duplicate row(s) collapsed",
                count=dedupe_result.identical_duplicates,
            )
        for key in dedupe_result.conflicting_keys:
            report.add(
                Severity.HARD,
                "key_conflict",
                f"same key with conflicting payloads: {key}",
            )


def _check_recognised_codes(report: ValidationReport) -> None:
    """Make unclassified codes visible in the audit record.

    Reads the codes already collected while counting rows, so the classification
    rules exist in exactly one place and cannot drift between here and the record.
    """
    for dimension in DIMENSIONS:
        values = report.unknown_codes.get(dimension)
        if values:
            report.add(
                Severity.WARN,
                f"unclassified_{dimension}",
                f"{len(values)} unclassified {dimension} code(s), ingested but excluded from "
                f"aggregates until the source's facet list confirms what they are: "
                f"{', '.join(sorted(values))}",
                count=len(values),
            )


def aggregate_publish_gate(
    records: Sequence[GenerationRecord], *, context: str = "aggregate publish"
) -> list[Finding]:
    """HARD findings that must block publishing these rows into an aggregate.

    Separate from :func:`validate` on purpose, because the two answer different
    questions. Ingestion asks *"can we trust this row enough to keep it?"* — and
    the answer for an unclassified code is yes, since dropping real source data
    is worse than carrying it labelled. Publishing an aggregate asks *"may this
    row contribute to a total?"* — and for an unclassified code the answer must be
    no. If EIA ships a new aggregate code and we default it to leaf, every national
    total silently double-counts, and no row-level test would fail.

    Call this before any mart or aggregate is published. Empty list means the
    rows are safe to aggregate.
    """
    seen: dict[str, set[str]] = {}
    for record in records:
        for dimension, code in unknown_codes(record).items():
            seen.setdefault(dimension, set()).add(code)

    return [
        Finding(
            Severity.HARD,
            f"unclassified_{dimension}",
            f"{context} blocked: {len(values)} unclassified {dimension} code(s) would "
            f"participate in an aggregate: {', '.join(sorted(values))}. Classify them "
            f"against the source's facet list before publishing.",
            count=len(values),
        )
        for dimension, values in seen.items()
    ]


def _check_completeness(
    report: ValidationReport,
    source_record_total: int | None,
    window_start: str,
    window_end: str,
) -> None:
    """Compare retrieved rows against the source's own count.

    Note what this does *not* do: it does not check the result against a
    Cartesian product of geographies x sectors x sources. That grid is sparse by
    design — 24.6% populated in July 2026, and Puerto Rico carries only 3 of 7
    sectors — so a completeness test built on the full grid would fail
    permanently on valid data.
    """
    if source_record_total is None:
        return
    if source_record_total > report.record_count:
        report.add(
            Severity.WARN,
            "truncated_extract",
            f"retrieved {report.record_count} of {source_record_total} rows the source "
            f"reported for {window_start}..{window_end}",
            count=source_record_total - report.record_count,
        )
    if not report.record_count:
        report.add(
            Severity.INFO,
            "empty_extract",
            f"no rows returned for {window_start}..{window_end}",
        )


def _add_observations(report: ValidationReport) -> None:
    """Measure the Phase 0 characteristics on every batch.

    This is deliberately not a set of assertions. It is the evidence trail: if a
    future run shows a null rate or negative rate that has moved sharply, the
    change is visible in the audit log rather than buried in a failed test that
    someone learns to ignore.
    """
    report.add(
        Severity.INFO,
        "generation_nulls",
        f"{report.null_generation_count} null generation value(s) preserved as null",
        count=report.null_generation_count,
    )
    report.add(
        Severity.INFO,
        "generation_negatives",
        f"{report.negative_generation_count} negative generation value(s) preserved "
        f"(storage/CHP net consumption is legitimately negative)",
        count=report.negative_generation_count,
    )
    report.add(
        Severity.INFO,
        "rollup_rows_retained",
        f"{report.rollup_row_count} rollup row(s) retained and labelled, not dropped",
        count=report.rollup_row_count,
    )
    report.add(
        Severity.INFO,
        "unknown_code_rows_retained",
        f"{report.unknown_row_count} row(s) with an unclassified dimension code, retained as "
        f"raw and excluded from aggregates by aggregate_publish_gate",
        count=report.unknown_row_count,
    )
