"""
Pure transformation functions for Job 3 football facts.

Callers materialize raw + processed-layer DataFrames and pass them in;
these functions do no I/O.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_fact_injury_event_football(
    raw_injuries_df: DataFrame,
    dim_injury_type_df: DataFrame,
    dim_player_df: DataFrame,
    gap_days: int = 14,
) -> DataFrame:
    """
    Football injury events from player injury records.

    football_player_injuries has one row per player per injury report with
    injury status. The same injury (e.g., "Hamstring") appears across multiple
    consecutive reports. Collapse consecutive reports into discrete injury
    events with start/end dates.

    Island-and-gap pattern:
      1. Filter to sports injuries only (join dim_injury_type, keep
         is_sports_injury=true)
      2. Sort by (player_id, injury_type, date)
      3. Detect gaps: if current date is > gap_days after previous for same
         player+injury, it's a new event (new island)
      4. groupBy island → one row per injury event with start/end/duration
    """
    with_injury_type = raw_injuries_df.join(
        dim_injury_type_df.select("raw_injury_text", "injury_type_id", "is_sports_injury"),
        F.col("injury_reason") == F.col("raw_injury_text"),
        how="left",
    )

    filtered = with_injury_type.filter(
        (F.col("is_sports_injury") == F.lit(True))
        & F.col("player_id").isNotNull()
        & F.col("from_date").isNotNull()
    ).select(
        F.col("player_id"),
        F.col("season_name"),
        F.col("injury_reason"),
        F.col("from_date").cast("date").alias("date_from"),
        F.col("end_date").cast("date").alias("date_until"),
        F.col("injury_type_id"),
    )

    w_order = Window.partitionBy("player_id", "injury_reason").orderBy("date_from")
    with_gap = (
        filtered
        .withColumn("_prev_date", F.lag("date_from").over(w_order))
        .withColumn(
            "_is_gap",
            F.col("_prev_date").isNull()
            | (F.datediff(F.col("date_from"), F.col("_prev_date")) > gap_days),
        )
        .withColumn(
            "_island_id",
            F.sum(F.when(F.col("_is_gap"), 1).otherwise(0)).over(w_order),
        )
    )

    collapsed = (
        with_gap
        .groupBy("player_id", "injury_reason", "injury_type_id", "_island_id")
        .agg(
            F.min("date_from").alias("injury_date"),
            F.max(F.coalesce(F.col("date_until"), F.col("date_from"))).alias("return_date"),
            F.min("season_name").alias("season_name"),
            F.count("*").alias("games_missed"),
        )
        .withColumn("days_missed", F.datediff(F.col("return_date"), F.col("injury_date")))
    )

    with_player = collapsed.join(
        dim_player_df.filter(F.col("sport") == F.lit("football")).select(
            F.col("player_id").alias("player_id_surrogate"),
            F.col("source_player_id").alias("player_id"),
        ),
        on="player_id",
        how="inner",
    )

    w_event_id = Window.orderBy("player_id_surrogate", "injury_date", "injury_reason")
    return (
        with_player
        .withColumn(
            "injury_event_id", F.row_number().over(w_event_id).cast("int")
        )
        .select(
            "injury_event_id",
            F.col("player_id_surrogate").alias("player_id"),
            "injury_type_id",
            F.lit("football").alias("sport"),
            F.col("season_name").cast("string").alias("season"),
            "injury_date",
            "return_date",
            "days_missed",
            "games_missed",
        )
    )


def build_fact_football_performance_seasonal(
    raw_performance_df: DataFrame, dim_player_df: DataFrame
) -> DataFrame:
    """
    Football player performance stats. One row per player per season/competition.
    Joins player_performances to dim_player for FK to surrogate player_id.
    """
    football_players = dim_player_df.filter(F.col("sport") == F.lit("football")).select(
        F.col("player_id"), F.col("source_player_id").alias("player_id_raw")
    )

    perf_renamed = raw_performance_df.withColumnRenamed("player_id", "src_player_id")
    joined = perf_renamed.join(
        football_players,
        perf_renamed["src_player_id"] == football_players["player_id_raw"],
        how="inner",
    )

    return (
        joined
        .select(
            F.col("player_id"),
            F.col("season_name").cast("string"),
            F.col("competition_name").cast("string"),
            F.col("team_name").cast("string"),
            F.col("goals").cast("int"),
            F.col("assists").cast("int"),
            F.col("minutes_played").cast("int"),
            # Appearances — Transfermarkt source uses nb_on_pitch
            F.col("nb_on_pitch").cast("int").alias("games_played"),
            F.col("yellow_cards").cast("int"),
            # Combine straight reds and second-yellow reds into a single red_cards metric
            (F.coalesce(F.col("direct_red_cards"), F.lit(0))
             + F.coalesce(F.col("second_yellow_cards"), F.lit(0))
            ).cast("int").alias("red_cards"),
            F.col("clean_sheets").cast("int"),
            F.col("goals_conceded").cast("int"),
        )
    )


def build_fact_football_market_value(
    raw_market_df: DataFrame, dim_player_df: DataFrame
) -> DataFrame:
    """
    Football player market value time series. One row per player per date.
    Converts Unix timestamp to proper date. Value is in euros.
    """
    football_players = dim_player_df.filter(F.col("sport") == F.lit("football")).select(
        F.col("player_id"), F.col("source_player_id").alias("player_id_raw")
    )

    market_renamed = raw_market_df.withColumnRenamed("player_id", "src_player_id")
    joined = market_renamed.join(
        football_players,
        market_renamed["src_player_id"] == football_players["player_id_raw"],
        how="inner",
    )

    return (
        joined
        .select(
            F.col("player_id"),
            F.from_unixtime(F.col("date_unix"), "yyyy-MM-dd").cast("date").alias("date"),
            F.col("value").cast("int").alias("market_value"),
        )
    )
