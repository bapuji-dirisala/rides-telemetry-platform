from __future__ import annotations

from rides_telemetry import __version__


def test_version_is_set() -> None:
    assert __version__
    assert isinstance(__version__, str)


def test_version_is_semver_like() -> None:
    parts = __version__.split(".")
    assert len(parts) == 3
    for part in parts:
        assert part.isdigit()
