"""Tests for glue_jobs/transforms/dimensions.py"""
from datetime import date

from pyspark.sql import Row

from transforms.dimensions import build_dim_player


def test_dim_player_takes_latest_roster_row_per_player(spark):
    """
    NFL rosters carry one row per player per (season, week). dim_player should
    reduce to the latest snapshot per gsis_id.
    """
    rosters = spark.createDataFrame(
        [
            # Same player, three snapshots — latest (2023 wk17) should win
            Row(gsis_id="00-01", season=2023, week=1, full_name="Player One",
                birth_date=date(1990, 1, 1), position="QB",
                height="6-2", weight="200", college="Old College"),
            Row(gsis_id="00-01", season=2023, week=17, full_name="Player One",
                birth_date=date(1990, 1, 1), position="QB",
                height="6-2", weight="205", college="Old College"),
            Row(gsis_id="00-01", season=2022, week=10, full_name="Player One",
                birth_date=date(1990, 1, 1), position="QB",
                height="6-2", weight="195", college="Old College"),
            # NULL gsis_id should be filtered out
            Row(gsis_id=None, season=2023, week=1, full_name="Ghost",
                birth_date=None, position=None,
                height=None, weight=None, college=None),
        ]
    )
    profiles = spark.createDataFrame(
        [
            Row(player_id="12345", player_name="Football Player",
                date_of_birth=date(1995, 5, 5), position="Forward",
                height="180cm", citizenship="Spain"),
        ]
    )

    result = build_dim_player(rosters, profiles).collect()

    # 1 NFL row (post-dedup) + 1 football row
    assert len(result) == 2

    nfl_row = next(r for r in result if r.sport == "nfl")
    assert nfl_row.source_player_id == "00-01"
    assert nfl_row.weight == "205"          # latest week's weight
    assert nfl_row.nationality is None      # never inferred for NFL

    football_row = next(r for r in result if r.sport == "football")
    assert football_row.source_player_id == "12345"
    assert football_row.nationality == "Spain"
    assert football_row.weight is None      # not in Transfermarkt profiles
    assert football_row.college is None


def test_dim_player_surrogate_keys_are_stable_and_ordered(spark):
    """
    player_id is a surrogate assigned via row_number ordered by
    (sport, source_player_id). Reruns with same data must produce same ids.
    """
    # Non-null placeholders so Spark can infer types on inference-only columns
    rosters = spark.createDataFrame(
        [
            Row(gsis_id="00-02", season=2023, week=1, full_name="B",
                birth_date=date(1990, 1, 1), position="X",
                height="6-0", weight="200", college="X"),
            Row(gsis_id="00-01", season=2023, week=1, full_name="A",
                birth_date=date(1990, 1, 1), position="X",
                height="6-0", weight="200", college="X"),
        ]
    )
    empty_profiles_schema = (
        "player_id string, player_name string, date_of_birth date, "
        "position string, height string, citizenship string"
    )
    profiles = spark.createDataFrame([], schema=empty_profiles_schema)

    result = build_dim_player(rosters, profiles).orderBy("player_id").collect()

    # Ordering: ("nfl", "00-01") -> id=1, ("nfl", "00-02") -> id=2
    assert result[0].source_player_id == "00-01"
    assert result[0].player_id == 1
    assert result[1].source_player_id == "00-02"
    assert result[1].player_id == 2
