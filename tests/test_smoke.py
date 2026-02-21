from __future__ import annotations

from rides_telemetry import __version__


def test_version_is_set() -> None:
    assert __version__
    assert isinstance(__version__, str)
