"""Tests for glue_jobs/transforms/football_facts.py"""
from datetime import date

from pyspark.sql import Row

from transforms.football_facts import build_fact_football_market_value


def test_market_value_unix_timestamp_converts_to_date(spark):
    """
    Raw source stores date as Unix timestamp (seconds since epoch).
    Transform must convert to a proper date column, join to dim_player for
    the surrogate player_id, and cast value to int.
    """
    raw_market = spark.createDataFrame(
        [
            # 2023-01-15 00:00:00 UTC
            Row(player_id="12345", date_unix=1673740800, value=1000000),
            # 2023-06-01 00:00:00 UTC
            Row(player_id="12345", date_unix=1685577600, value=1500000),
        ]
    )
    dim_player = spark.createDataFrame(
        [Row(player_id=100, source_player_id="12345", sport="football")]
    )

    result = (
        build_fact_football_market_value(raw_market, dim_player)
        .orderBy("date")
        .collect()
    )

    assert len(result) == 2
    assert result[0].date == date(2023, 1, 15)
    assert result[0].player_id == 100                # surrogate, not raw string
    assert result[0].market_value == 1_000_000
    assert result[1].date == date(2023, 6, 1)
    assert result[1].market_value == 1_500_000
