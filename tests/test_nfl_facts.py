"""
Tests for glue_jobs/transforms/nfl_facts.py

Focus is on the injury-collapsing logic — the trickiest transform in the
pipeline. Weekly injury reports get grouped into discrete events using an
"island and gap" pattern; these tests verify the boundary behaviors.
"""
from datetime import date

from pyspark.sql import Row

from transforms.nfl_facts import build_fact_injury_event_nfl


def _dim_injury_type(spark):
    return spark.createDataFrame(
        [
            Row(raw_injury_text="Hamstring", injury_type_id=1, is_sports_injury=True),
            Row(raw_injury_text="Coach's Decision", injury_type_id=99, is_sports_injury=False),
        ]
    )


def _dim_player(spark):
    return spark.createDataFrame(
        [Row(player_id=100, source_player_id="00-01", sport="nfl")]
    )


def test_consecutive_weekly_reports_within_gap_collapse_to_one_event(spark):
    """
    Three weekly reports for the same player + injury, spaced 7 days apart,
    should collapse to ONE event spanning min→max date.
    """
    raw = spark.createDataFrame(
        [
            Row(gsis_id="00-01", report_primary_injury="Hamstring",
                date_modified=date(2023, 9, 10), report_status="Out", season=2023),
            Row(gsis_id="00-01", report_primary_injury="Hamstring",
                date_modified=date(2023, 9, 17), report_status="Out", season=2023),
            Row(gsis_id="00-01", report_primary_injury="Hamstring",
                date_modified=date(2023, 9, 24), report_status="Questionable", season=2023),
        ]
    )

    result = build_fact_injury_event_nfl(
        raw, _dim_injury_type(spark), _dim_player(spark)
    ).collect()

    assert len(result) == 1
    event = result[0]
    assert event.player_id == 100
    assert event.injury_type_id == 1
    assert event.sport == "nfl"
    assert event.injury_date == date(2023, 9, 10)
    assert event.return_date == date(2023, 9, 24)
    # games_missed counts Out/Doubtful only (2 of 3 reports)
    assert event.games_missed == 2
    # days_missed = max(datediff=14, games_missed*7=14) = 14
    assert event.days_missed == 14


def test_reports_beyond_gap_threshold_split_into_two_events(spark):
    """
    A 30-day gap between reports for the same injury should be treated as a
    re-injury — two separate events, not one long span.
    """
    raw = spark.createDataFrame(
        [
            Row(gsis_id="00-01", report_primary_injury="Hamstring",
                date_modified=date(2023, 9, 10), report_status="Out", season=2023),
            Row(gsis_id="00-01", report_primary_injury="Hamstring",
                date_modified=date(2023, 9, 17), report_status="Out", season=2023),
            # 30-day gap after the previous row — new island
            Row(gsis_id="00-01", report_primary_injury="Hamstring",
                date_modified=date(2023, 10, 17), report_status="Out", season=2023),
        ]
    )

    result = build_fact_injury_event_nfl(
        raw, _dim_injury_type(spark), _dim_player(spark)
    ).collect()

    assert len(result) == 2


def test_non_sports_injuries_are_filtered_out(spark):
    """
    Rows joined to a dim_injury_type row with is_sports_injury=False
    ("Coach's Decision", "Not injury related") must be excluded from the fact.
    """
    raw = spark.createDataFrame(
        [
            Row(gsis_id="00-01", report_primary_injury="Coach's Decision",
                date_modified=date(2023, 9, 10), report_status="Out", season=2023),
            Row(gsis_id="00-01", report_primary_injury="Hamstring",
                date_modified=date(2023, 9, 10), report_status="Out", season=2023),
        ]
    )

    result = build_fact_injury_event_nfl(
        raw, _dim_injury_type(spark), _dim_player(spark)
    ).collect()

    assert len(result) == 1
    assert result[0].injury_type_id == 1  # Only the Hamstring event survives
