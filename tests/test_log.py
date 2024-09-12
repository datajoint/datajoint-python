def test_log(schema_any):
    ts, events = (schema_any.log & 'event like "Declared%%"').fetch(
        "timestamp", "event"
    )
    assert len(ts) >= 2
