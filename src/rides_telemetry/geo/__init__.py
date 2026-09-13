"""NYC geography helpers (borough centroids, bounding boxes).

Phase 1 uses borough-level resolution — every event's coordinates are
sampled inside a small radius around the borough centroid. This is
deliberately coarse: fine-grained taxi-zone / H3-hex geography lands
in phase 6, where it actually gets used for surge computation.
"""

from __future__ import annotations

from rides_telemetry.geo.nyc import (
    NYC_BBOX,
    NYC_BOROUGHS,
    Borough,
    is_inside_nyc,
    jitter_point,
)

__all__ = ["NYC_BBOX", "NYC_BOROUGHS", "Borough", "is_inside_nyc", "jitter_point"]
