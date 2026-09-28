"""Unit tests. These run in CI with no broker, no database and no network."""

import json
import sys
import uuid
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "consumer"))


def make_event(event_type="device.heartbeat", device_id="dev-1"):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "payload": {"device_id": device_id, "value": 42.0},
    }


def test_event_serialises_as_valid_json():
    event = make_event()
    assert json.loads(json.dumps(event))["event_id"] == event["event_id"]


def test_duplicate_detection_returns_true_on_second_call():
    """This is the behaviour the whole idempotency design depends on."""
    import consumer

    fake = MagicMock()
    fake.set.return_value = None            # key already existed
    consumer._r = fake
    assert consumer.is_duplicate("abc") is True

    fake.set.return_value = True            # key was newly created
    assert consumer.is_duplicate("abc") is False


def test_zone_lookup_uses_cache_when_present():
    import consumer

    fake = MagicMock()
    fake.get.return_value = "zone-3"
    consumer._r = fake

    zone, was_cached = consumer.lookup_zone("dev-1")
    assert zone == "zone-3"
    assert was_cached is True
    fake.setex.assert_not_called()


def test_zone_lookup_writes_cache_on_miss():
    import consumer

    fake = MagicMock()
    fake.get.return_value = None
    consumer._r = fake

    _zone, was_cached = consumer.lookup_zone("dev-1")
    assert was_cached is False
    fake.setex.assert_called_once()


def test_process_releases_dedup_key_when_handler_fails():
    """A failed event must not stay marked as processed, or retries short-circuit
    as duplicates and the DLQ path is unreachable (the guide's original bug)."""
    import consumer

    fake = MagicMock()
    fake.set.return_value = True            # first time seen
    consumer._r = fake
    consumer._db = None

    poison = make_event(event_type="poison")

    class FakeCursor:
        def execute(self, *_a, **_k):
            raise ValueError("deliberate failure")

    class FakeDb:
        closed = 0
        autocommit = False

        def cursor(self):
            return FakeCursor()

    consumer._db = FakeDb()

    import pytest

    with pytest.raises(ValueError):
        consumer.process(json.dumps(poison).encode())

    fake.delete.assert_called_once_with(f"processed:{poison['event_id']}")
