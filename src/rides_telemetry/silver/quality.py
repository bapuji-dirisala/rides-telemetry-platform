"""Row-level quality predicates as reusable Spark ``Column`` expressions.

Kept out of the streaming jobs so:

- The predicates can be unit-tested against a tiny hermetic DataFrame
  without running any streaming machinery.
- A reviewer can see every filter in one place without scrolling
  through Delta writer configuration.
- Tuning a threshold means editing one constant in
  :mod:`~rides_telemetry.silver.schemas`, not chasing filters
  scattered across multiple jobs.

Each helper returns a boolean ``Column`` that is ``True`` when the row
should be **kept**. Callers combine them with ``&`` and pass the result
to :meth:`DataFrame.filter`.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F  # noqa: N812

from rides_telemetry.geo.nyc import NYC_BBOX
from rides_telemetry.silver.schemas import (
    MAX_ACCEPTABLE_ACCURACY_M,
    MAX_PLAUSIBLE_SPEED_KMH,
)


def has_valid_coordinates(lat_col: str = "lat", lng_col: str = "lng") -> Column:
    """Coordinates are present, finite, and inside the NYC bounding box.

    We reject ``NULL`` and ``NaN`` explicitly — Spark treats ``NaN``
    as neither equal to itself nor comparable in the usual way, so
    a naive range predicate would silently let NaN rows through.
    """
    min_lat, min_lng, max_lat, max_lng = NYC_BBOX
    lat = F.col(lat_col)
    lng = F.col(lng_col)
    return (
        lat.isNotNull()
        & lng.isNotNull()
        & ~F.isnan(lat)
        & ~F.isnan(lng)
        & lat.between(min_lat, max_lat)
        & lng.between(min_lng, max_lng)
    )


def has_plausible_speed(speed_col: str = "speed_kmh") -> Column:
    """Speed is either absent (device didn't report) or below the cap.

    Missing speed is *fine* — a lot of low-end devices don't send a
    Doppler read. What we're filtering out is device glitches that
    report 900 km/h.
    """
    speed = F.col(speed_col)
    return speed.isNull() | (~F.isnan(speed) & (speed >= 0) & (speed <= MAX_PLAUSIBLE_SPEED_KMH))


def has_usable_accuracy(accuracy_col: str = "accuracy_m") -> Column:
    """Reported GPS accuracy is either absent or fine enough to be useful."""
    accuracy = F.col(accuracy_col)
    return accuracy.isNull() | (
        ~F.isnan(accuracy) & (accuracy >= 0) & (accuracy <= MAX_ACCEPTABLE_ACCURACY_M)
    )


def gps_ping_is_valid() -> Column:
    """Composite predicate applied to bronze GPS rows before writing silver.

    A row is kept iff **all** individual predicates pass. Written as
    a single expression so Spark's Catalyst optimiser can push it
    down into the Delta scan.
    """
    return has_valid_coordinates() & has_plausible_speed() & has_usable_accuracy()
