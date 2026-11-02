import pytest

from batch.schema import validate_columns


def test_validate_columns_passes_when_present(make_df):
    df = make_df([("order_id", "string"), ("customer_id", "string"), ("product", "string"),
                  ("amount", "double"), ("status", "string"), ("event_ts", "timestamp")],
                 [("o1", "c1", "A", 1.0, "PAID", "2026-01-01 09:00:00")])
    validate_columns(df)  # should not raise


def test_validate_columns_raises_when_missing(make_df):
    df = make_df([("order_id", "string"), ("amount", "double")], [("o1", 1.0)])
    with pytest.raises(ValueError):
        validate_columns(df)
