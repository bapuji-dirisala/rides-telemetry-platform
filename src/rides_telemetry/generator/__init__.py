"""Ride event generator.

Turns :class:`~rides_telemetry.sources.RawTrip` records into a stream of
:class:`~rides_telemetry.events.Event` instances that mirror what a real
ride-hailing platform would emit onto Kafka.

Public surface:

- :class:`RideSimulator` — the generator itself.
- :class:`SimulatorConfig` — knobs (ping interval, jitter, seed…).
"""

from __future__ import annotations

from rides_telemetry.generator.simulator import RideSimulator, SimulatorConfig

__all__ = ["RideSimulator", "SimulatorConfig"]
