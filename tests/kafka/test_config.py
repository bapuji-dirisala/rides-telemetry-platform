"""KafkaConfig rendering tests."""

from __future__ import annotations

from rides_telemetry.kafka import KafkaConfig


def test_default_config_has_idempotence_and_acks_all() -> None:
    cfg = KafkaConfig().to_confluent_config()
    assert cfg["enable.idempotence"] is True
    assert cfg["acks"] == "all"
    assert cfg["bootstrap.servers"] == "localhost:19092"


def test_extra_settings_override() -> None:
    cfg = KafkaConfig(extra={"security.protocol": "SASL_SSL"}).to_confluent_config()
    assert cfg["security.protocol"] == "SASL_SSL"


def test_topic_names_are_versioned() -> None:
    cfg = KafkaConfig()
    # Consumers depend on these names; a rename here should be a
    # deliberate, tests-updating change.
    assert cfg.topic_trips == "rides.trips.v1"
    assert cfg.topic_gps == "rides.gps.v1"
