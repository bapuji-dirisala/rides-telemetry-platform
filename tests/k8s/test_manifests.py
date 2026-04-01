"""Phase 9+10 — static validation of committed manifests."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_producer_manifests_present():
    k8s = ROOT / "k8s" / "producer"
    for name in (
        "Dockerfile",
        "deployment.yaml",
        "service.yaml",
        "configmap.yaml",
        "hpa.yaml",
        "kustomization.yaml",
    ):
        assert (k8s / name).exists(), f"missing {name}"


def test_dockerfile_runs_as_non_root():
    txt = (ROOT / "k8s" / "producer" / "Dockerfile").read_text()
    assert "USER 10001" in txt
    assert "useradd" in txt


def test_deployment_has_security_hardening():
    txt = (ROOT / "k8s" / "producer" / "deployment.yaml").read_text()
    assert "runAsNonRoot: true" in txt
    assert "readOnlyRootFilesystem: true" in txt
    assert "drop: [ALL]" in txt
    assert "allowPrivilegeEscalation: false" in txt


def test_hpa_scales_on_consumer_lag():
    txt = (ROOT / "k8s" / "producer" / "hpa.yaml").read_text()
    assert "rides_bronze_consumer_lag_messages" in txt
    assert "maxReplicas: 5" in txt


def test_databricks_workflow_bundle_defines_medallion():
    txt = (ROOT / "databricks" / "workflows" / "rides_streaming.yml").read_text()
    for task in ("bronze_gps", "bronze_trips", "silver_matched", "gold_h3"):
        assert task in txt
    assert "bundle:" in txt
    assert "targets:" in txt


def test_unity_catalog_sql_present():
    uc = ROOT / "databricks" / "unity-catalog"
    assert (uc / "01_catalogs_schemas.sql").exists()
    assert (uc / "02_grants.sql").exists()
    grants = (uc / "02_grants.sql").read_text()
    assert "rides_analysts" in grants
    assert "GRANT SELECT ON SCHEMA rides_dev.gold" in grants
