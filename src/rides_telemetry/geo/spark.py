"""Spark-native geo helpers.

Kept separate from :mod:`~rides_telemetry.geo.nyc` (which is pure
Python) so importing the geo package doesn't require pyspark on the
path — event generation, tests, and the producer all work without it.

The Catalyst-native helpers (``borough_of_expr``, ``haversine_km_expr``)
are pushed down into Spark SQL and execute inside Catalyst's generated
Java code with no Python-UDF overhead. The one exception is
``h3_index_of_expr``, which uses a vectorised ``pandas_udf`` because
there is no native Java H3 implementation in the pyspark stack — see
its docstring for the trade-off.
"""

from __future__ import annotations

import math
from functools import lru_cache
from typing import Any, cast

import h3
import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

from rides_telemetry.geo.nyc import NYC_BBOX, NYC_BOROUGHS

# Earth's mean radius in km — the standard Haversine constant.
_EARTH_RADIUS_KM = 6371.0088


# Precomputed conversion factors — 1° lat is ~111 km everywhere;
# 1° lng shrinks with latitude and at NYC (~40.7°N) is ~84 km.
# Squaring these on the difference gives a scaled-Euclidean distance
# in km², which is fine for nearest-centroid classification within
# a small metro area (we don't need great-circle for 30 km ranges).
_KM_PER_DEG_LAT = 111.0
_KM_PER_DEG_LNG_AT_NYC = 111.0 * math.cos(math.radians(40.7))


def borough_of_expr(lat_col: str = "lat", lng_col: str = "lng") -> Column:
    """Attribute a lat/lng pair to its nearest NYC borough centroid.

    Returns a ``Column`` whose value is one of the borough names in
    :data:`~rides_telemetry.geo.nyc.NYC_BOROUGHS` (``Manhattan``,
    ``Brooklyn``, ``Queens``, ``Bronx``, ``Staten Island``, ``EWR``),
    or ``NULL`` if the point falls outside :data:`NYC_BBOX`.

    The classification is nearest-centroid (a Voronoi tessellation of
    the six centroids) — accurate to about a city block near
    centroids, and progressively fuzzier at borough boundaries. That's
    fine for surge / demand marts where the aggregation window is
    already coarser than the noise. Phase 6 replaces this with H3
    hex indexing for real geospatial precision.
    """
    lat = F.col(lat_col)
    lng = F.col(lng_col)

    # Scaled squared distance to each centroid, in km².
    distances = {
        name: (
            F.pow((lat - b.lat) * F.lit(_KM_PER_DEG_LAT), F.lit(2))
            + F.pow((lng - b.lng) * F.lit(_KM_PER_DEG_LNG_AT_NYC), F.lit(2))
        )
        for name, b in NYC_BOROUGHS.items()
    }

    min_dist = F.least(*distances.values())

    # Chained CASE WHEN: the first centroid whose distance equals the
    # min wins. Ties are essentially impossible with floating point,
    # but if they occurred, the borough dict's insertion order (which
    # is preserved in Python 3.7+) determines the winner.
    picked = F.lit(None).cast(StringType())
    for name, d in distances.items():
        picked = F.when(d == min_dist, F.lit(name)).otherwise(picked)

    # Defensive: silver already filters non-NYC pings via
    # `has_valid_coordinates`, but the bbox guard here documents the
    # contract and protects against a caller passing a raw bronze row.
    min_lat, min_lng, max_lat, max_lng = NYC_BBOX
    inside_bbox = lat.between(min_lat, max_lat) & lng.between(min_lng, max_lng)
    return F.when(inside_bbox, picked).otherwise(F.lit(None).cast(StringType()))


def haversine_km_expr(
    lat1_col: str,
    lng1_col: str,
    lat2_col: str,
    lng2_col: str,
) -> Column:
    """Great-circle distance in km between two lat/lng points.

    Standard Haversine formula, expressed in pure Spark SQL:

    .. math::

        a = \\sin^2\\left(\\tfrac{\\Delta\\phi}{2}\\right) +
            \\cos(\\phi_1)\\cos(\\phi_2)\\sin^2\\left(\\tfrac{\\Delta\\lambda}{2}\\right)

        d = 2R \\arcsin(\\sqrt{a})

    where :math:`\\phi` is latitude in radians and :math:`\\lambda` is
    longitude in radians. :math:`R = 6371.0088` km is Earth's mean
    radius.

    Used by :mod:`~rides_telemetry.gold.fraud_teleport` to compute
    the implied speed between two consecutive pings. Kept as a
    reusable helper because future marts (matching-quality, route
    deviation) will need it too.

    Why not just scaled-Euclidean like ``borough_of_expr``?
    Borough classification only cares about *relative* distances
    between centroids (nearest wins), so a scaled approximation is
    fine. Fraud detection needs *absolute* distances that we then
    divide by time to get km/h — the ~5% error at NYC latitude of
    scaled-Euclidean would map straight into the fraud threshold
    and produce false positives (or misses).
    """
    phi1 = F.radians(F.col(lat1_col))
    phi2 = F.radians(F.col(lat2_col))
    d_phi = F.radians(F.col(lat2_col) - F.col(lat1_col))
    d_lambda = F.radians(F.col(lng2_col) - F.col(lng1_col))

    a = F.pow(F.sin(d_phi / F.lit(2.0)), F.lit(2)) + (
        F.cos(phi1) * F.cos(phi2) * F.pow(F.sin(d_lambda / F.lit(2.0)), F.lit(2))
    )
    # ``asin(sqrt(a))`` — clamp ``a`` at 1.0 to avoid domain errors
    # from floating-point noise (a can be a tiny bit > 1.0 for
    # antipodal points, which we'll never see for NYC pings but the
    # clamp is essentially free).
    a_clamped = F.least(a, F.lit(1.0))
    return F.lit(2.0 * _EARTH_RADIUS_KM) * F.asin(F.sqrt(a_clamped))


# -------------------------------------------------------------------------
# H3 hex indexing (uber-h3 python bindings via pandas_udf).
# -------------------------------------------------------------------------


@lru_cache(maxsize=16)
def _h3_udf(resolution: int) -> Any:
    """Build (or fetch cached) a pandas_udf for a given H3 resolution.

    The resolution is baked into the closure so pyspark can register
    exactly one UDF per resolution used in the job. In practice we
    only ever call with ``resolution=8`` (the phase-6 default), so
    exactly one UDF gets registered per SparkSession.
    """

    # pyspark's ``pandas_udf`` decorator has notoriously loose stubs;
    # mypy can't resolve the overload for the ``@pandas_udf(returnType)``
    # form. We know at runtime this returns a ``UserDefinedFunctionLike``.
    @pandas_udf(StringType())  # type: ignore[call-overload]
    def _udf(lat: pd.Series, lng: pd.Series) -> pd.Series:
        # h3-py v4 doesn't ship a vectorised scalar API, so this
        # loops in Python inside the Arrow batch. For our demo scale
        # (22k rows) that's <100 ms of overhead per batch; production
        # workloads would swap this for the ``h3-pandas`` extension
        # or a Sedona Java UDF.
        out = []
        for la, ln in zip(lat, lng, strict=True):
            if pd.notna(la) and pd.notna(ln):
                out.append(h3.latlng_to_cell(float(la), float(ln), resolution))
            else:
                out.append(None)
        return pd.Series(out, dtype=object)

    return _udf


def h3_index_of_expr(
    lat_col: str = "lat",
    lng_col: str = "lng",
    resolution: int = 8,
) -> Column:
    """Compute the H3 cell index for a ``(lat, lng)`` at a given resolution.

    Returns a ``Column`` of ``StringType`` — H3 cell IDs are 15-char
    hex strings (e.g. ``"882a100d67fffff"``). ``NULL`` in either
    input propagates to ``NULL`` in the output.

    Resolution reference (edge length, cell area):

    ==========  =============  =============
    Resolution  Edge length    Cell area
    ==========  =============  =============
    7           1.2 km         5.2 km²
    8           461 m          0.74 km²   ← Phase 6 default
    9           174 m          0.11 km²
    10          65 m           15,000 m²
    ==========  =============  =============

    **Res 8 is the classic ride-hail default** — small enough to
    resolve individual streets in Manhattan, large enough that
    borough-scale queries hit thousands (not millions) of hexes.

    **Why a ``pandas_udf`` and not a Catalyst-native expression?**
    There is no pure-Spark implementation of H3. Alternatives:

    - ``pandas_udf`` (this) — Arrow-batched, ~0-copy across the
      JVM boundary. Python-loop overhead is bounded by the batch
      size (~10k rows/batch by default).
    - Java UDF from Apache Sedona (``ST_H3CellIDs``) — fastest,
      but adds Sedona as a Maven/JAR dependency and shifts the
      whole geospatial stack to Sedona conventions.
    - Regular ``udf`` — slower than ``pandas_udf`` by 10-50× due
      to per-row serialisation overhead.

    ``pandas_udf`` is the right point on the trade-off curve for a
    demo lakehouse; Sedona would be the production choice.

    **Why the memoized factory?** pyspark registers a UDF into the
    session on first use. Recreating the ``pandas_udf`` on every
    ``h3_index_of_expr`` call would leak session state; the
    ``@lru_cache`` ensures we register exactly one UDF per unique
    ``resolution`` value.
    """
    return cast(Column, _h3_udf(resolution)(F.col(lat_col), F.col(lng_col)))
