from batch.transforms import clean_orders, daily_revenue, dedupe_latest, incremental_filter

SCHEMA = [
    ("order_id", "string"),
    ("customer_id", "string"),
    ("product", "string"),
    ("amount", "double"),
    ("status", "string"),
    ("event_ts", "timestamp"),
]


def test_dedupe_latest_keeps_most_recent(make_df):
    df = make_df(SCHEMA, [
        ("o1", "c1", "A", 10.0, "PAID", "2026-01-01 09:00:00"),
        ("o1", "c1", "A", 12.0, "SHIPPED", "2026-01-01 10:00:00"),
    ])
    rows = dedupe_latest(df).collect()
    assert len(rows) == 1
    assert rows[0]["amount"] == 12.0
    assert rows[0]["status"] == "SHIPPED"


def test_clean_orders_drops_invalid_and_dedupes(make_df):
    df = make_df(SCHEMA, [
        ("o1", "c1", "A", 10.0, "PAID", "2026-01-01 09:00:00"),
        ("o1", "c1", "A", 12.0, "SHIPPED", "2026-01-01 10:00:00"),   # dup -> keep latest
        ("o2", "c2", "B", -5.0, "PAID", "2026-01-01 09:00:00"),      # bad amount
        ("o3", None, "B", 5.0, "PAID", "2026-01-01 09:00:00"),       # null customer
        ("o4", "c4", "B", 7.0, "BOGUS", "2026-01-01 09:00:00"),      # bad status
        ("o5", "c5", "A", 3.0, "PLACED", "2026-01-02 09:00:00"),
    ])
    rows = {r["order_id"]: r for r in clean_orders(df).collect()}
    assert set(rows) == {"o1", "o5"}
    assert rows["o1"]["amount"] == 12.0


def test_incremental_filter_keeps_only_newer(make_df):
    df = make_df(SCHEMA, [
        ("a", "c", "A", 1.0, "PAID", "2026-01-01 09:00:00"),
        ("b", "c", "A", 1.0, "PAID", "2026-01-01 10:00:00"),
    ])
    rows = incremental_filter(df, "2026-01-01 09:30:00").collect()
    assert [r["order_id"] for r in rows] == ["b"]


def test_daily_revenue_aggregates_per_day_and_product(make_df):
    df = make_df(SCHEMA, [
        ("o1", "c1", "A", 10.0, "PAID", "2026-01-01 09:00:00"),
        ("o2", "c2", "A", 20.0, "PAID", "2026-01-01 11:00:00"),
        ("o3", "c3", "B", 5.0, "PAID", "2026-01-02 09:00:00"),
    ])
    rows = {(str(r["ds"]), r["product"]): r for r in daily_revenue(df).collect()}
    assert rows[("2026-01-01", "A")]["revenue"] == 30.0
    assert rows[("2026-01-01", "A")]["orders"] == 2
    assert rows[("2026-01-02", "B")]["revenue"] == 5.0
    assert rows[("2026-01-02", "B")]["orders"] == 1
