"""NYC borough centroids and bounding box.

Coordinates come from Wikipedia's "borough of X, New York City"
infobox for each of the five boroughs. They're accurate to about
0.01° (~1 km) which is more than enough for phase 1 — we're not
routing, we're just producing plausible lat/lng values inside the
correct borough.

The bounding box covers all five boroughs plus a small margin.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Borough:
    """A single NYC borough's centroid and effective radius."""

    name: str
    lat: float
    lng: float
    # Approximate radius in km within which we're happy to jitter a
    # sampled point. Deliberately smaller than the borough's true
    # radius so we don't accidentally sample a point that falls in a
    # different borough.
    radius_km: float


NYC_BOROUGHS: dict[str, Borough] = {
    "Manhattan": Borough("Manhattan", 40.7831, -73.9712, radius_km=3.5),
    "Brooklyn": Borough("Brooklyn", 40.6782, -73.9442, radius_km=5.0),
    "Queens": Borough("Queens", 40.7282, -73.7949, radius_km=6.0),
    "Bronx": Borough("Bronx", 40.8448, -73.8648, radius_km=4.5),
    "Staten Island": Borough("Staten Island", 40.5795, -74.1502, radius_km=5.0),
    # "EWR" is TLC's shorthand for Newark Airport, which shows up as a
    # legal pickup zone in HVFHS data. Map it to a small circle around
    # the actual airport so we don't drop those trips.
    "EWR": Borough("EWR", 40.6895, -74.1745, radius_km=2.0),
}

NYC_BBOX: tuple[float, float, float, float] = (40.45, -74.30, 40.95, -73.65)
"""(min_lat, min_lng, max_lat, max_lng) covering all five boroughs + EWR."""


# 1 degree of latitude ≈ 111 km everywhere on Earth.
# 1 degree of longitude ≈ 111 km * cos(latitude) — at NYC latitude
# (~40.7°) that's about 84 km.
_KM_PER_DEG_LAT = 111.0
_KM_PER_DEG_LNG_AT_NYC = 111.0 * math.cos(math.radians(40.7))


def jitter_point(
    borough: Borough,
    rng: random.Random,
    max_km: float | None = None,
) -> tuple[float, float]:
    """Sample a (lat, lng) uniformly inside a disk around the borough centroid.

    Uses the standard sqrt-uniform trick so the sampling is area-uniform
    rather than radially-biased (points aren't clumped near the centre).
    """
    max_km = max_km if max_km is not None else borough.radius_km
    r_km = max_km * math.sqrt(rng.random())
    theta = rng.uniform(0.0, 2 * math.pi)

    d_lat = (r_km * math.sin(theta)) / _KM_PER_DEG_LAT
    d_lng = (r_km * math.cos(theta)) / _KM_PER_DEG_LNG_AT_NYC
    return borough.lat + d_lat, borough.lng + d_lng


def is_inside_nyc(lat: float, lng: float) -> bool:
    """True if ``(lat, lng)`` is inside the NYC bounding box."""
    min_lat, min_lng, max_lat, max_lng = NYC_BBOX
    return min_lat <= lat <= max_lat and min_lng <= lng <= max_lng
