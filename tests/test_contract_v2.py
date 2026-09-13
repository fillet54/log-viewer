import pytest

from cinc.core.contract import validate_events


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
