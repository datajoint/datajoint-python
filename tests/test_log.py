from . import schema


def test_log():
    ts, events = (schema.schema.log & 'event like "Declared%%"').fetch(
        "timestamp", "event"
    )
    assert len(ts) >= 2
