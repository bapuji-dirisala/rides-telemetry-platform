"""Hermetic silver-schema tests.

No Spark session needed — these just introspect ``StructType`` metadata
and constants. Guards the invariants that:

- Silver drops the Kafka envelope columns (that's what "layer promotion"
  means; leaving them would just be a rename of bronze).
- Silver GPS carries every business field from bronze GPS.
- Silver trip facts have the derived ``status`` column and cover every
  lifecycle timestamp.
- Quality thresholds are within sane operational ranges — a typo like
  ``MAX_PLAUSIBLE_SPEED_KMH = 20`` (should be 200) would silently drop
  90%+ of traffic in production; this catches it in CI.
"""

from __future__ import annotations

from rides_telemetry.bronze.schemas import GPS_PAYLOAD_SCHEMA, KAFKA_ENVELOPE_FIELDS
from rides_telemetry.events import (
    GpsPing,
    TripAccepted,
    TripCancelled,
    TripEnded,
    TripRequested,
    TripStarted,
)
from rides_telemetry.silver.schemas import (
    GPS_WATERMARK,
    MATCHED_JOIN_END_SLACK,
    MATCHED_JOIN_START_SLACK,
    MATCHED_TRIP_WATERMARK,
    MAX_ACCEPTABLE_ACCURACY_M,
    MAX_PLAUSIBLE_SPEED_KMH,
    SILVER_GPS_SCHEMA,
    SILVER_MATCHED_SCHEMA,
    SILVER_TRIPS_SCHEMA,
    TRIP_WATERMARK,
)


class TestSilverGpsSchema:
    """Silver GPS carries every business field and no Kafka envelope."""

    def test_covers_every_gps_ping_field(self) -> None:
        pydantic_fields = set(GpsPing.model_fields.keys())
        # Silver drops event_type (all rows are gps_ping by definition).
        pydantic_fields.discard("event_type")

        spark_fields = {f.name for f in SILVER_GPS_SCHEMA.fields}
        missing = pydantic_fields - spark_fields
        assert not missing, (
            f"silver GPS schema is missing pydantic fields: {missing}. "
            "Add them to SILVER_GPS_SCHEMA in rides_telemetry/silver/schemas.py."
        )

    def test_drops_kafka_envelope_columns(self) -> None:
        # Silver is business-facing; Kafka envelope belongs to ingestion.
        envelope_names = {f.name for f in KAFKA_ENVELOPE_FIELDS}
        spark_fields = {f.name for f in SILVER_GPS_SCHEMA.fields}
        leaked = envelope_names & spark_fields
        # ``ingest_ts`` is intentionally kept — silver needs it to compute
        # end-to-end latency to gold. Everything else must go.
        leaked.discard("ingest_ts")
        assert not leaked, f"silver GPS schema still carries Kafka envelope columns: {leaked}"

    def test_has_silver_processed_ts(self) -> None:
        names = {f.name for f in SILVER_GPS_SCHEMA.fields}
        assert "silver_processed_ts" in names, (
            "silver rows must carry their own processing timestamp for lag monitoring"
        )

    def test_gps_payload_and_silver_agree_on_business_fields(self) -> None:
        # Non-envelope, non-derived fields must match bronze GPS exactly.
        bronze_business = {f.name for f in GPS_PAYLOAD_SCHEMA.fields} - {"event_type"}
        silver_business = {f.name for f in SILVER_GPS_SCHEMA.fields} - {
            "ingest_ts",
            "silver_processed_ts",
        }
        assert bronze_business == silver_business, (
            f"drift between bronze/silver GPS: only in bronze={bronze_business - silver_business}, "
            f"only in silver={silver_business - bronze_business}"
        )


class TestSilverTripsSchema:
    """Silver trip facts fold five event types into one row per trip."""

    def test_covers_all_lifecycle_event_types_via_timestamps(self) -> None:
        # Every lifecycle event type should show up as a ``*_ts`` column.
        expected = {
            "requested_ts",
            "accepted_ts",
            "started_ts",
            "ended_ts",
            "cancelled_ts",
        }
        names = {f.name for f in SILVER_TRIPS_SCHEMA.fields}
        missing = expected - names
        assert not missing, f"silver trip facts missing lifecycle timestamps: {missing}"

    def test_carries_every_geography_and_outcome_field(self) -> None:
        # Union of fields on the concrete lifecycle models (excluding the
        # envelope fields event_id/event_ts/event_type, which don't map
        # 1:1 to the fact-row shape).
        lifecycle_fields: set[str] = set()
        for m in (TripRequested, TripAccepted, TripStarted, TripEnded, TripCancelled):
            lifecycle_fields.update(m.model_fields.keys())
        lifecycle_fields -= {"event_id", "event_ts", "event_type"}

        names = {f.name for f in SILVER_TRIPS_SCHEMA.fields}
        missing = lifecycle_fields - names
        assert not missing, f"silver trip facts missing lifecycle fields: {missing}"

    def test_has_derived_status_column(self) -> None:
        names = {f.name for f in SILVER_TRIPS_SCHEMA.fields}
        assert "status" in names, "silver trip facts must expose the derived status"

    def test_trip_id_is_not_nullable(self) -> None:
        # trip_id is the MERGE key; a null would break upserts silently.
        trip_id_field = next(f for f in SILVER_TRIPS_SCHEMA.fields if f.name == "trip_id")
        assert not trip_id_field.nullable, "trip_id is the MERGE key, must be NOT NULL"

    def test_lifecycle_timestamps_are_all_nullable(self) -> None:
        # A trip might reach silver with only some events processed.
        for ts in ("requested_ts", "accepted_ts", "started_ts", "ended_ts", "cancelled_ts"):
            field = next(f for f in SILVER_TRIPS_SCHEMA.fields if f.name == ts)
            assert field.nullable, f"{ts} must be nullable — partial-state trips exist"


class TestSilverMatchedSchema:
    """Silver matched carries both the ping and its trip context."""

    def test_carries_full_gps_ping(self) -> None:
        # Every field from silver GPS (except silver_processed_ts, which
        # matched re-generates for its own layer) should be present.
        gps_fields = {f.name for f in SILVER_GPS_SCHEMA.fields}
        gps_fields.discard("silver_processed_ts")

        matched_fields = {f.name for f in SILVER_MATCHED_SCHEMA.fields}
        missing = gps_fields - matched_fields
        assert not missing, f"silver matched drops GPS fields: {missing}"

    def test_carries_trip_context_fields(self) -> None:
        required = {
            "rider_id",
            "trip_status",
            "trip_requested_ts",
            "trip_started_ts",
            "trip_ended_ts",
            "pickup_lat",
            "pickup_lng",
            "dropoff_lat",
            "dropoff_lng",
        }
        matched_fields = {f.name for f in SILVER_MATCHED_SCHEMA.fields}
        missing = required - matched_fields
        assert not missing, f"silver matched missing trip context: {missing}"

    def test_trip_started_ts_is_not_nullable(self) -> None:
        # The join pre-filters trips with started_ts IS NOT NULL, so
        # every matched row must have a start timestamp.
        started = next(f for f in SILVER_MATCHED_SCHEMA.fields if f.name == "trip_started_ts")
        assert not started.nullable, "matched rows only exist for started trips"

    def test_event_id_is_matched_row_pk(self) -> None:
        # event_id is unique per GPS ping, and the join produces at most
        # one matched row per (event_id, trip_id) — but trip_id is fixed
        # per event_id in silver GPS, so event_id is effectively the PK.
        event_id = next(f for f in SILVER_MATCHED_SCHEMA.fields if f.name == "event_id")
        assert not event_id.nullable


class TestQualityThresholds:
    """Sanity-check the tunable constants so a typo can't ship silently."""

    def test_speed_cap_is_in_realistic_range(self) -> None:
        # Highway max in NYC is ~110 km/h; anything under 100 would
        # drop real traffic. Anything over 500 would let obvious
        # bogus values through.
        assert 100.0 < MAX_PLAUSIBLE_SPEED_KMH < 500.0

    def test_accuracy_cap_is_in_realistic_range(self) -> None:
        # 50m is roughly how good a consumer GPS is; 500m would still
        # be useful for coarse aggregation. Anything under 20 would
        # drop most urban pings (multipath in Manhattan is brutal).
        assert 20.0 < MAX_ACCEPTABLE_ACCURACY_M < 2000.0

    def test_watermarks_are_reasonable_durations(self) -> None:
        # Just make sure they parse as Spark durations and pick sane
        # units — "10 minutes", "2 hours", not "10 hours" for GPS.
        assert GPS_WATERMARK.endswith(("minute", "minutes"))
        assert TRIP_WATERMARK.endswith(("hour", "hours"))

    def test_matched_join_slack_is_reasonable(self) -> None:
        # Start slack < 1 minute (clock skew, not app buffering).
        # End slack < 15 minutes (post-drop pings, not next trip's).
        assert MATCHED_JOIN_START_SLACK.endswith(("second", "seconds"))
        assert MATCHED_JOIN_END_SLACK.endswith(("minute", "minutes"))
        assert MATCHED_TRIP_WATERMARK.endswith(("minute", "minutes", "hour", "hours"))
