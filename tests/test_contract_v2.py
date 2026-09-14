import pytest

from cinc.core.contract import validate_events
from cinc.core.assemble import assemble_page_data


def valid(row_id=1):
    return {
        "row_id": row_id,
        "time": "2026-01-01T00:00:00Z",
        "log_type": "demo",
    }


def test_contract_accepts_valid_events():
    validate_events([valid()], "demo")


@pytest.mark.parametrize("field", ["row_id", "time", "log_type"])
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


def test_assembly_includes_declared_plugin_styles():
    from cinc.plugins.core_event import CoreEventLogType

    payload = {
        "channels": ["A"],
        "events": [{"channels": ["A"], "utctime": "2026-01-01T00:00:00Z"}],
    }
    page = assemble_page_data([(CoreEventLogType(), payload)])
    assert any(".ce-row" in style for style in page["view"]["styles"])


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
