"""
Pure transformation functions for Job 2 NFL facts.

Callers materialize raw + processed-layer DataFrames and pass them in;
these functions do no I/O.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_fact_injury_event_nfl(
    raw_injuries_df: DataFrame,
    dim_injury_type_df: DataFrame,
    dim_player_df: DataFrame,
    gap_days: int = 14,
) -> DataFrame:
    """
    NFL injury events from weekly injury reports.

    nfl_injuries has one row per player per week with injury status. The same
    injury (e.g., "Hamstring") appears across multiple weeks. Collapse those
    weekly observations into discrete injury events with start/end dates.

    Two judgment calls baked in (both defensible, both tunable via `gap_days`):

    1. GAP_DAYS = 14 (default). Two reports for the same player+injury are
       part of the same event if they are within 14 days of each other. Matches
       the football collapsing logic in Job 3 for symmetry. A wider window
       merges genuine re-injuries; a narrower one fragments single events.

    2. Duration floor = games_missed * 7. NFL reports give us observation dates
       (date_modified), not injury spans. A single-row island has min == max,
       so naive datediff = 0 even if the player was held Out for that week.
       Flooring at games_missed * 7 attributes at least one week of duration
       per missed game. Events where the player was never Out/Doubtful keep
       days_missed = 0 — they represent Questionable/Probable reports where
       the player played through, and analytical queries can filter them out.

    Island-and-gap implementation: lag(date_modified) over the partition, flag
    a "new island" when gap > gap_days, cumulative-sum the flags to assign an
    island ID, then groupBy island ID.
    """
    with_injury_type = raw_injuries_df.join(
        dim_injury_type_df.select("raw_injury_text", "injury_type_id", "is_sports_injury"),
        F.col("report_primary_injury") == F.col("raw_injury_text"),
        how="left",
    )

    filtered = with_injury_type.filter(
        (F.col("is_sports_injury") == F.lit(True))
        & F.col("gsis_id").isNotNull()
        & F.col("date_modified").isNotNull()
    ).select(
        F.col("gsis_id"),
        F.col("report_primary_injury"),
        F.col("date_modified").cast("date").alias("date_modified"),
        F.col("report_status"),
        F.col("injury_type_id"),
        F.col("season").cast("int").alias("season"),
    )

    w_order = Window.partitionBy("gsis_id", "report_primary_injury").orderBy(
        "date_modified"
    )
    with_gap = (
        filtered
        .withColumn("_prev_date", F.lag("date_modified").over(w_order))
        .withColumn(
            "_is_gap",
            F.col("_prev_date").isNull()
            | (F.datediff(F.col("date_modified"), F.col("_prev_date")) > gap_days),
        )
        .withColumn(
            "_island_id",
            F.sum(F.when(F.col("_is_gap"), 1).otherwise(0)).over(w_order),
        )
    )

    collapsed = (
        with_gap
        .groupBy("gsis_id", "report_primary_injury", "injury_type_id", "_island_id")
        .agg(
            F.min("date_modified").alias("injury_date"),
            F.max("date_modified").alias("return_date"),
            F.min("season").alias("season"),
            F.sum(
                F.when(
                    F.col("report_status").isin(F.lit("Out"), F.lit("Doubtful")), 1
                ).otherwise(0)
            ).cast("int").alias("games_missed"),
        )
        .withColumn(
            "days_missed",
            F.greatest(
                F.datediff(F.col("return_date"), F.col("injury_date")),
                F.col("games_missed") * F.lit(7),
            ),
        )
    )

    with_player = collapsed.join(
        dim_player_df.filter(F.col("sport") == F.lit("nfl")).select(
            F.col("player_id"), F.col("source_player_id").alias("gsis_id")
        ),
        on="gsis_id",
        how="inner",
    )

    w_event_id = Window.orderBy("player_id", "injury_date", "report_primary_injury")
    return (
        with_player
        .withColumn(
            "injury_event_id", F.row_number().over(w_event_id).cast("int")
        )
        .select(
            "injury_event_id",
            "player_id",
            "injury_type_id",
            F.lit("nfl").alias("sport"),
            F.col("season").cast("string").alias("season"),
            "injury_date",
            "return_date",
            "days_missed",
            "games_missed",
        )
    )


def build_fact_nfl_performance_weekly(
    raw_stats_df: DataFrame, dim_player_df: DataFrame
) -> DataFrame:
    """
    NFL player weekly performance stats. One row per player per week.
    Joins player_stats to dim_player for FK to surrogate player_id.
    Removes columns that belong in dim_player (headshot_url, position, etc.).
    """
    nfl_players = dim_player_df.filter(F.col("sport") == F.lit("nfl")).select(
        F.col("player_id"), F.col("source_player_id").alias("player_id_raw")
    )

    # raw_stats.player_id is the gsis_id; rename to avoid collision with the
    # surrogate player_id from dim_player after the join.
    stats_renamed = raw_stats_df.withColumnRenamed("player_id", "gsis_id_raw")
    joined = stats_renamed.join(
        nfl_players,
        stats_renamed["gsis_id_raw"] == nfl_players["player_id_raw"],
        how="inner",
    )

    return (
        joined
        .select(
            F.col("player_id"),
            F.col("season").cast("int"),
            F.col("week").cast("int"),
            F.col("season_type").cast("string"),
            F.col("opponent_team").cast("string"),
            F.col("completions").cast("int"),
            F.col("attempts").cast("int"),
            F.col("passing_yards").cast("int"),
            F.col("passing_tds").cast("int"),
            F.col("interceptions").cast("int"),
            F.col("sacks").cast("float"),
            F.col("carries").cast("int"),
            F.col("rushing_yards").cast("int"),
            F.col("rushing_tds").cast("int"),
            F.col("receptions").cast("int"),
            F.col("targets").cast("int"),
            F.col("receiving_yards").cast("int"),
            F.col("receiving_tds").cast("int"),
            F.col("passing_epa").cast("float"),
            F.col("rushing_epa").cast("float"),
            F.col("receiving_epa").cast("float"),
            F.col("target_share").cast("float"),
            F.col("fantasy_points").cast("float"),
            F.col("fantasy_points_ppr").cast("float"),
        )
    )


def build_fact_nfl_performance_seasonal(weekly_df: DataFrame) -> DataFrame:
    """
    Aggregate weekly stats to seasonal. Totals for counting stats (yards, TDs),
    averages for rate stats (EPA, target share).
    """
    return (
        weekly_df
        .groupBy("player_id", "season")
        .agg(
            F.count("week").cast("int").alias("games_played"),
            F.sum("completions").cast("int").alias("total_completions"),
            F.sum("attempts").cast("int").alias("total_attempts"),
            F.sum("passing_yards").cast("int").alias("total_passing_yards"),
            F.sum("passing_tds").cast("int").alias("total_passing_tds"),
            F.sum("interceptions").cast("int").alias("total_interceptions"),
            F.sum("carries").cast("int").alias("total_carries"),
            F.sum("rushing_yards").cast("int").alias("total_rushing_yards"),
            F.sum("rushing_tds").cast("int").alias("total_rushing_tds"),
            F.sum("receptions").cast("int").alias("total_receptions"),
            F.sum("targets").cast("int").alias("total_targets"),
            F.sum("receiving_yards").cast("int").alias("total_receiving_yards"),
            F.sum("receiving_tds").cast("int").alias("total_receiving_tds"),
            F.avg("passing_epa").cast("float").alias("avg_passing_epa"),
            F.avg("rushing_epa").cast("float").alias("avg_rushing_epa"),
            F.avg("receiving_epa").cast("float").alias("avg_receiving_epa"),
            F.sum("fantasy_points").cast("float").alias("total_fantasy_points"),
            F.sum("fantasy_points_ppr").cast("float").alias("total_fantasy_points_ppr"),
        )
    )
