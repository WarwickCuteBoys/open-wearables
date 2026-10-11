from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

from app.schemas.enums import HealthScoreCategory
from app.services.providers.whoop.data_247 import Whoop247Data


def _provider() -> Whoop247Data:
    return Whoop247Data("whoop", "https://api.prod.whoop.com", MagicMock())


def _cycle(**overrides: Any) -> dict[str, Any]:
    cycle = {
        "score_state": "SCORED",
        "start": "2026-09-09T00:00:00+07:00",
        "end": None,
        "timezone_offset": "+07:00",
        "score": {"kilojoule": 1000, "strain": 12.5},
    }
    cycle.update(overrides)
    return cycle


def test_open_scored_cycle_saves_partial_energy_without_strain() -> None:
    energy, strain = _provider().normalize_cycle(_cycle(), uuid4())

    assert energy is not None
    assert energy.recorded_at == datetime.fromisoformat("2026-09-09T00:00:00+07:00")
    assert energy.value > 0
    assert energy.interval_end is not None
    assert energy.interval_end > energy.recorded_at
    assert energy.source_type == "whoop_cycle_partial"
    assert energy.coverage_known is True
    assert energy.is_daily_total is True
    assert strain is None


def test_closed_scored_cycle_saves_final_energy_and_strain() -> None:
    end = "2026-09-10T00:00:00+07:00"
    energy, strain = _provider().normalize_cycle(_cycle(end=end), uuid4())

    assert energy is not None
    assert energy.source_type == "whoop_cycle_final"
    assert energy.interval_end == datetime.fromisoformat(end)
    assert strain is not None
    assert strain.category == HealthScoreCategory.STRAIN


def test_unscored_cycle_is_ignored() -> None:
    energy, strain = _provider().normalize_cycle(_cycle(score_state="PENDING"), uuid4())

    assert energy is None
    assert strain is None


def test_cycle_with_invalid_interval_is_ignored() -> None:
    energy, strain = _provider().normalize_cycle(_cycle(end="2026-09-08T23:59:00+07:00"), uuid4())

    assert energy is None
    assert strain is None


def test_open_cycle_started_in_future_is_ignored() -> None:
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    energy, strain = _provider().normalize_cycle(_cycle(start=future.isoformat()), uuid4())

    assert energy is None
    assert strain is None
