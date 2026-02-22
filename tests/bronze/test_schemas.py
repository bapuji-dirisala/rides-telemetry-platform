"""Bronze-schema consistency tests.

These tests are hermetic — they just introspect the pydantic model
metadata and the Spark ``StructType`` fields. No Spark session needed,
so they run in the default (fast) pytest invocation and guard against
someone adding a pydantic field without updating the Spark schema.
"""

from __future__ import annotations

from rides_telemetry.bronze.schemas import (
    BRONZE_GPS_SCHEMA,
    BRONZE_TRIPS_SCHEMA,
    GPS_PAYLOAD_SCHEMA,
    KAFKA_ENVELOPE_FIELDS,
    TRIPS_PAYLOAD_SCHEMA,
)
from rides_telemetry.events import (
    GpsPing,
    TripAccepted,
    TripCancelled,
    TripEnded,
    TripRequested,
    TripStarted,
)


def _pydantic_field_names(*models: type) -> set[str]:
    """Union of every field name across the given pydantic models."""
    names: set[str] = set()
    for m in models:
        names.update(m.model_fields.keys())
    return names


class TestTripsSchema:
    """The bronze trips payload schema must cover every lifecycle model."""

    def test_covers_every_lifecycle_field(self) -> None:
        pydantic_fields = _pydantic_field_names(
            TripRequested, TripAccepted, TripStarted, TripEnded, TripCancelled
        )
        spark_fields = {f.name for f in TRIPS_PAYLOAD_SCHEMA.fields}
        missing = pydantic_fields - spark_fields
        assert not missing, (
            f"Bronze trips schema is missing pydantic fields: {missing}. "
            "Add them to TRIPS_PAYLOAD_SCHEMA in rides_telemetry/bronze/schemas.py."
        )

    def test_no_extra_fields(self) -> None:
        # Extra Spark fields aren't an error per se, but they usually
        # mean a rename got missed.
        pydantic_fields = _pydantic_field_names(
            TripRequested, TripAccepted, TripStarted, TripEnded, TripCancelled
        )
        spark_fields = {f.name for f in TRIPS_PAYLOAD_SCHEMA.fields}
        extra = spark_fields - pydantic_fields
        assert not extra, f"Bronze trips schema has fields with no pydantic equivalent: {extra}."


class TestGpsSchema:
    def test_covers_every_gps_ping_field(self) -> None:
        pydantic_fields = _pydantic_field_names(GpsPing)
        spark_fields = {f.name for f in GPS_PAYLOAD_SCHEMA.fields}
        missing = pydantic_fields - spark_fields
        assert not missing, f"Bronze GPS schema missing pydantic fields: {missing}"

    def test_no_extra_fields(self) -> None:
        pydantic_fields = _pydantic_field_names(GpsPing)
        spark_fields = {f.name for f in GPS_PAYLOAD_SCHEMA.fields}
        extra = spark_fields - pydantic_fields
        assert not extra, f"Bronze GPS schema has extra fields: {extra}"


class TestKafkaEnvelope:
    def test_bronze_tables_include_envelope(self) -> None:
        envelope_names = {f.name for f in KAFKA_ENVELOPE_FIELDS}
        assert envelope_names.issubset({f.name for f in BRONZE_TRIPS_SCHEMA.fields})
        assert envelope_names.issubset({f.name for f in BRONZE_GPS_SCHEMA.fields})

    def test_envelope_carries_operational_fields(self) -> None:
        envelope_names = {f.name for f in KAFKA_ENVELOPE_FIELDS}
        # These are the columns downstream debugging depends on — bronze
        # tables would be much less useful without them.
        for required in (
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingest_ts",
            "raw_json",
        ):
            assert required in envelope_names, f"envelope missing {required!r}"
