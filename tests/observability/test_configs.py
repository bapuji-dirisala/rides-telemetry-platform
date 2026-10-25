"""Phase 11+12 — static validation of observability + CI/CD artifacts."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_prometheus_scrape_covers_producer_and_broker():
    txt = (ROOT / "observability" / "prometheus" / "prometheus.yml").read_text()
    for job in ("rides-producer", "redpanda", "spark-streaming"):
        assert f"job_name: {job}" in txt


def test_alert_rules_define_slos():
    txt = (ROOT / "observability" / "alerts" / "rides.yml").read_text()
    for alert in (
        "ProducerThroughputDropped",
        "BronzeConsumerLagHigh",
        "BronzeStreamingNoProgress",
        "EventLatencyP99Over30s",
    ):
        assert alert in txt


def test_grafana_dashboard_is_valid_json():
    doc = json.loads((ROOT / "observability" / "grafana" / "rides-dashboard.json").read_text())
    assert doc["title"].startswith("Rides Telemetry")
    assert len(doc["panels"]) >= 5


def test_jenkinsfile_runs_full_pipeline():
    txt = (ROOT / "jenkins" / "Jenkinsfile").read_text()
    for stage in ("install", "lint", "typecheck", "test", "terraform validate"):
        assert f"stage('{stage}')" in txt


def test_terraform_workflow_runs_fmt_and_validate():
    txt = (ROOT / ".github" / "workflows" / "terraform.yml").read_text()
    assert "terraform -chdir=terraform fmt -recursive -check -diff" in txt
    assert "terraform -chdir=terraform validate" in txt
