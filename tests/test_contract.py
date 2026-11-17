import pytest

from cinc.core.contract import validate_events
from cinc.core.assemble import assemble_page_data


def valid(row_id=1):
    return {
        "row_id": row_id,
        "row_key": f"demo:{row_id}",
        "time": "2026-01-01T00:00:00Z",
        "log_type": "demo",
    }


def test_contract_accepts_valid_events():
    validate_events([valid()], "demo")


@pytest.mark.parametrize("field", ["row_id", "row_key", "time", "log_type"])
def test_contract_rejects_missing_fields(field):
    event = valid()
    del event[field]
    with pytest.raises(ValueError, match="demo"):
        validate_events([event], "demo")


def test_contract_rejects_bad_time_and_duplicates():
    event = valid()
    event["time"] = "not-a-time"
    with pytest.raises(ValueError, match="demo"):
        validate_events([event], "demo")
    with pytest.raises(ValueError, match="duplicate"):
        validate_events([valid(), valid()], "demo")
    other = valid(2)
    other["row_key"] = "demo:1"
    with pytest.raises(ValueError, match="duplicate row_key"):
        validate_events([valid(), other], "demo")


def test_types_and_augmenters_may_not_touch_row_key():
    from cinc.plugins.text_log import TextLogType

    payload = {"lines": [{"ts": "2026-01-01T00:00:00Z", "message": "x"}]}
    events = assemble_page_data([(TextLogType(), payload)])["logData"]["events"]
    assert events[0]["row_key"] == "text_log:0"
    log_type = TextLogType()
    log_type.register_event_augmenter(
        "rekey", lambda events, context: [{**e, "row_key": "x"} for e in events]
    )
    with pytest.raises(ValueError, match="changed row_key"):
        assemble_page_data([(log_type, payload)])


def test_assembly_includes_declared_plugin_styles():
    from cinc.plugins.core_event import CoreEventLogType

    payload = {
        "channels": ["A"],
        "events": [{"channels": ["A"], "utctime": "2026-01-01T00:00:00Z"}],
    }
    page = assemble_page_data([(CoreEventLogType(), payload)])
    assert any(".ce-channel" in style for style in page["view"]["styles"])


def test_assembly_rejects_plugin_supplied_log_type():
    from cinc.plugins.text_log import TextLogType

    payload = {
        "lines": [
            {
                "ts": "2026-01-01T00:00:00Z",
                "message": "bad",
                "log_type": "other",
            }
        ]
    }
    with pytest.raises(ValueError, match="must not supply log_type"):
        assemble_page_data([(TextLogType(), payload)])


def test_event_augmenters_run_in_order_and_protect_identity():
    from cinc.plugins.text_log import TextLogType

    log_type = TextLogType()
    log_type.register_event_augmenter(
        "first",
        lambda events, context: [{**event, "derived": "one"} for event in events],
    )
    log_type.register_event_augmenter(
        "second",
        lambda events, context: [
            {**event, "derived": event["derived"] + "-two"} for event in events
        ],
    )
    payload = {"lines": [{"ts": "2026-01-01T00:00:00Z", "message": "x"}]}
    events = assemble_page_data([(log_type, payload)])["logData"]["events"]
    assert events[0]["derived"] == "one-two"
    with pytest.raises(ValueError, match="duplicate augmenter"):
        log_type.register_event_augmenter("first", lambda events, context: events)


def test_core_event_header_config_is_validated():
    from cinc.plugins.core_event import config

    with pytest.raises(ValueError, match="tone"):
        config.severities({"severities": [{"name": "Bad", "tone": "purple"}]})
    with pytest.raises(ValueError, match="field path"):
        config.series({"series": [{"id": "x", "field": "data..x"}]})
    assert config.series({}) == []
    assert [s["tone"] for s in config.severities({})] == list(config.TONES)


def _order(times):
    from cinc.plugins.text_log import TextLogType

    payload = {"lines": [{"ts": t, "message": t} for t in times]}
    events = assemble_page_data([(TextLogType(), payload)])["logData"]["events"]
    return [event["message"] for event in events]


def test_events_are_ordered_by_instant_not_by_string():
    assert _order(["2026-01-01T00:00:05.500Z", "2026-01-01T00:00:05Z"]) == [
        "2026-01-01T00:00:05Z",
        "2026-01-01T00:00:05.500Z",
    ]
    assert _order(["2026-01-01T00:00:05Z", "2026-01-01T01:00:04+01:00"]) == [
        "2026-01-01T01:00:04+01:00",
        "2026-01-01T00:00:05Z",
    ]


@pytest.mark.parametrize(
    "value,micro",
    [
        ("2026-01-01T00:00:05.5Z", 500000),
        ("2026-01-01T00:00:05.1234567Z", 123456),
        ("2026-01-01T00:00:05,25+00:00", 250000),
        ("2026-01-01T00:00:05z", 0),
    ],
)
def test_parse_time_accepts_any_fraction(value, micro):
    from cinc.core.times import parse_time

    parsed = parse_time(value)
    assert parsed.microsecond == micro and parsed.utcoffset().total_seconds() == 0
    event = valid()
    event["time"] = value
    validate_events([event], "demo")


def test_parse_time_zone_rules():
    from cinc.core.times import MissingZoneError, parse_time

    assert parse_time("2026-01-01T00:00:05").tzinfo is not None
    with pytest.raises(MissingZoneError):
        parse_time("2026-01-01T00:00:05", require_zone=True)
    for bad in ("", "nope", None, 5):
        with pytest.raises(ValueError):
            parse_time(bad)
    event = valid()
    event["time"] = "2026-01-01T00:00:05"
    with pytest.raises(ValueError, match="timezone"):
        validate_events([event], "demo")
