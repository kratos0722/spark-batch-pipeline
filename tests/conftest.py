import os
import sys

import pytest

from batch.spark_session import build_spark


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = build_spark("tests", master="local[1]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


@pytest.fixture
def make_df(spark):
    """Build a DataFrame from rows via `spark.sql` (JVM-side: no Python workers
    and no file I/O, so it runs on any OS without winutils)."""
    def _make(schema: list[tuple[str, str]], rows: list[tuple]):
        selects = []
        for row in rows:
            cols = ", ".join(
                f"CAST({_literal(v)} AS {typ}) AS {name}"
                for (name, typ), v in zip(schema, row)
            )
            selects.append(f"SELECT {cols}")
        return spark.sql(" UNION ALL ".join(selects))

    return _make
