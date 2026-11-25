"""Static checks on the pipeline SQL files."""
import re
from pathlib import Path

import pytest

TRANSFORMATIONS = Path(__file__).resolve().parents[1] / "transformations"
SQL_FILES = ["01_bronze.sql", "02_silver.sql", "03_gold.sql"]


def read_sql(name):
    return (TRANSFORMATIONS / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("name", SQL_FILES)
def test_no_hardcoded_catalog(name):
    # The catalog comes from the bundle target, never from the SQL itself
    assert "veloradar." not in read_sql(name).lower()


def test_bronze_reads_landing_volume_from_variable():
    sql = read_sql("01_bronze.sql")
    assert "STREAM read_files" in sql
    assert "${source_path}" in sql


def test_silver_has_four_expectations_that_drop_rows():
    sql = read_sql("02_silver.sql")
    assert sql.count("EXPECT (") == 4
    assert sql.count("ON VIOLATION DROP ROW") == 4


def test_silver_deduplicates_on_kafka_offset():
    sql = read_sql("02_silver.sql")
    assert "AUTO CDC INTO silver.events" in sql
    assert re.search(r"KEYS\s*\(\s*kafka_partition\s*,\s*kafka_offset\s*\)", sql)


def test_gold_defines_the_three_materialized_views():
    sql = read_sql("03_gold.sql")
    views = set(re.findall(r"MATERIALIZED VIEW\s+gold\.(\w+)", sql))
    assert views == {"passes_by_hour", "speed_by_direction", "speed_distribution"}
