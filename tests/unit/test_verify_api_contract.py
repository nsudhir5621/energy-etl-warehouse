"""Offline regression tests for facet diagnostics, without publication or network access."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from energy_pipeline.config import Settings


@pytest.fixture
def verifier(monkeypatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "src"))
    spec = importlib.util.spec_from_file_location(
        "verify_api_contract", root / "scripts" / "verify_api_contract.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def forbid_network(*args, **kwargs):
        pytest.fail("Verifier tests must not make real HTTP requests")

    monkeypatch.setattr(module.requests.sessions.Session, "request", forbid_network)
    return module


def _facets(verifier, monkeypatch, **overrides):
    members = {"location": {"AK", "DC", "PR", "US"}, "sectorid": {"1"}, "fueltypeid": {"NG"}}
    members.update(overrides)
    monkeypatch.setattr(
        verifier,
        "fetch_facet_members",
        lambda settings, name: (members[name], dict.fromkeys(members[name], "fixture label")),
    )


@pytest.mark.parametrize("location", ["AK", "DC", "PR", "US", "PACC", "PAC"])
def test_recognized_geography_does_not_claim_publication(verifier, monkeypatch, capsys, location):
    _facets(verifier, monkeypatch, location={location})
    assert verifier.report_facets(Settings(api_key=None)) is True
    output = capsys.readouterr().out
    assert "facet schema/contract: PASS" in output
    assert "classification: PASS" in output
    assert "warehouse publication eligibility: NOT VERIFIED" in output


@pytest.mark.parametrize(
    "dimension, code", [("location", "ZZ"), ("sectorid", "8"), ("fueltypeid", "BIO")]
)
def test_unknown_facet_blocks_eligibility(verifier, monkeypatch, capsys, dimension, code):
    _facets(verifier, monkeypatch, **{dimension: {code}})
    assert verifier.report_facets(Settings(api_key=None)) is False
    output = capsys.readouterr().out
    assert f"{dimension} UNKNOWN: ['{code}']" in output
    assert "classification: FAIL" in output
    assert "warehouse publication eligibility: BLOCKED" in output


def test_empty_facet_fails_contract_without_publication_claim(verifier, monkeypatch, capsys):
    _facets(verifier, monkeypatch, location=set())
    assert verifier.report_facets(Settings(api_key=None)) is False
    output = capsys.readouterr().out
    assert "facet schema/contract: FAIL" in output
    assert "warehouse publication eligibility: NOT VERIFIED" in output


@pytest.mark.parametrize("unknown, expected", [(True, 1), (False, 0)])
def test_cli_verdict_distinguishes_diagnostics_from_publication(
    verifier, monkeypatch, capsys, unknown, expected
):
    _facets(verifier, monkeypatch, location={"ZZ" if unknown else "AK"})
    monkeypatch.setenv("EIA_API_KEY", "test-key")
    monkeypatch.setattr(verifier.sys, "argv", ["verify_api_contract.py", "--skip-uniqueness"])
    assert verifier.main() == expected
    output = capsys.readouterr().out
    assert f"DIAGNOSTIC VERDICT: {'FAIL' if unknown else 'PASS'}" in output
    assert "warehouse publication eligibility: PASS" not in output


def test_skipped_facets_do_not_imply_classification_or_publication(verifier, monkeypatch, capsys):
    monkeypatch.setenv("EIA_API_KEY", "test-key")
    monkeypatch.setattr(
        verifier.sys, "argv", ["verify_api_contract.py", "--skip-uniqueness", "--skip-facets"]
    )
    assert verifier.main() == 0
    output = capsys.readouterr().out
    assert "classification: NOT CHECKED" in output
    assert "warehouse publication eligibility: NOT VERIFIED" in output


@pytest.mark.parametrize(
    "payload", [{}, {"response": {"facets": None}}, {"response": {"facets": [{}]}}]
)
def test_malformed_facet_envelope_is_contract_failure(verifier, monkeypatch, capsys, payload):
    monkeypatch.setattr(
        verifier.requests,
        "get",
        lambda *args, **kwargs: SimpleNamespace(status_code=200, json=lambda: payload),
    )
    with pytest.raises(SystemExit, match="facet schema/contract: FAIL"):
        verifier.fetch_facet_members(Settings(api_key=None), "location")
    assert "facet location retrieval: PASS (HTTP 200)" in capsys.readouterr().out


def test_facet_request_failure_does_not_expose_request_url(verifier, monkeypatch):
    def fail_request(*args, **kwargs):
        raise verifier.requests.ConnectionError("request URL contained a private credential")

    monkeypatch.setattr(verifier.requests, "get", fail_request)
    with pytest.raises(SystemExit, match="facet location retrieval: FAIL") as error:
        verifier.fetch_facet_members(Settings(api_key=None), "location")
    assert "private credential" not in str(error.value)
