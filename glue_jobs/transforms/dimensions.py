"""
Pure transformation functions for Job 1 dimensions.

These functions take DataFrames and return DataFrames — no GlueContext, no
S3 reads, no side effects. Callers (job1_dimensions.py in prod, pytest in
tests) are responsible for materializing the inputs.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_injury_type(injury_lookup_df: DataFrame) -> DataFrame:
    """
    Build dim_injury_type from the raw lookup CSV DataFrame.

    Lookup CSV is the source of truth for injury classification — never
    hardcode these mappings in the job. Surrogate key ordered by
    (sport, raw_injury_text) so reruns produce stable ids across NFL/football
    fact joins.
    """
    typed = injury_lookup_df.select(
        F.col("sport").cast("string").alias("sport"),
        F.col("raw_injury_text").cast("string").alias("raw_injury_text"),
        F.col("body_region").cast("string").alias("body_region"),
        F.col("injury_category").cast("string").alias("injury_category"),
        F.col("severity").cast("string").alias("severity"),
        # CSV stores "true"/"false" as text — convert to actual boolean
        (F.lower(F.trim(F.col("is_sports_injury"))) == F.lit("true"))
        .alias("is_sports_injury"),
    )

    win = Window.orderBy("sport", "raw_injury_text")
    return (
        typed
        .withColumn("injury_type_id", F.row_number().over(win).cast("int"))
        .select(
            "injury_type_id",
            "body_region",
            "injury_category",
            "severity",
            "raw_injury_text",
            "sport",
            "is_sports_injury",
        )
    )


def build_dim_player(rosters_df: DataFrame, profiles_df: DataFrame) -> DataFrame:
    """
    Conformed player dim across sports.

    NFL rosters carry one row per player per (season, week) — reduce to the
    latest snapshot per gsis_id so the dim reflects most recent metadata.
    Football profiles are already keyed by player_id; dedupe defensively.

    Nationality is NULL for NFL (the source doesn't carry it — never infer).
    Weight and college are NULL for football (not in Transfermarkt profiles).
    """
    latest_roster = Window.partitionBy("gsis_id").orderBy(
        F.desc("season"), F.desc("week")
    )
    nfl = (
        rosters_df
        .filter(F.col("gsis_id").isNotNull())
        .withColumn("_rn", F.row_number().over(latest_roster))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("gsis_id").cast("string").alias("source_player_id"),
            F.lit("nfl").alias("sport"),
            F.col("full_name").cast("string").alias("player_name"),
            F.col("birth_date").cast("date").alias("date_of_birth"),
            F.col("position").cast("string").alias("position"),
            F.col("height").cast("string").alias("height"),
            F.col("weight").cast("string").alias("weight"),
            F.lit(None).cast("string").alias("nationality"),
            F.col("college").cast("string").alias("college"),
        )
    )

    football = (
        profiles_df
        .filter(F.col("player_id").isNotNull())
        .dropDuplicates(["player_id"])
        .select(
            F.col("player_id").cast("string").alias("source_player_id"),
            F.lit("football").alias("sport"),
            F.col("player_name").cast("string").alias("player_name"),
            F.col("date_of_birth").cast("date").alias("date_of_birth"),
            F.col("position").cast("string").alias("position"),
            F.col("height").cast("string").alias("height"),
            F.lit(None).cast("string").alias("weight"),
            F.col("citizenship").cast("string").alias("nationality"),
            F.lit(None).cast("string").alias("college"),
        )
    )

    unioned = nfl.unionByName(football)

    win = Window.orderBy("sport", "source_player_id")
    return (
        unioned
        .withColumn("player_id", F.row_number().over(win).cast("int"))
        .select(
            "player_id",
            "source_player_id",
            "sport",
            "player_name",
            "date_of_birth",
            "position",
            "height",
            "weight",
            "nationality",
            "college",
        )
    )


def build_dim_nfl_combine(
    combine_df: DataFrame,
    rosters_df: DataFrame,
    dim_player_df: DataFrame,
) -> DataFrame:
    """
    Combine -> dim_player FK chain: nfl_combine.pfr_id -> nfl_rosters.pfr_id
    -> nfl_rosters.gsis_id -> dim_player.source_player_id (sport='nfl').

    Inner joins drop combine participants who never made an NFL roster. That
    is expected — dim_nfl_combine only describes athletes who reached the league.
    Natural key is (player_id, season); no surrogate needed.
    """
    pfr_to_gsis = (
        rosters_df
        .filter(F.col("pfr_id").isNotNull() & F.col("gsis_id").isNotNull())
        .select("pfr_id", "gsis_id")
        .dropDuplicates(["pfr_id"])
    )

    nfl_players = (
        dim_player_df
        .filter(F.col("sport") == F.lit("nfl"))
        .select(
            F.col("player_id"),
            F.col("source_player_id").alias("gsis_id"),
        )
    )

    return (
        combine_df
        .filter(F.col("pfr_id").isNotNull())
        .join(pfr_to_gsis, on="pfr_id", how="inner")
        .join(nfl_players, on="gsis_id", how="inner")
        .select(
            F.col("player_id"),
            F.col("season").cast("int").alias("season"),
            F.col("pos").cast("string").alias("pos"),
            F.col("school").cast("string").alias("school"),
            F.col("forty").cast("float").alias("forty"),
            F.col("bench").cast("int").alias("bench"),
            F.col("vertical").cast("float").alias("vertical"),
            F.col("broad_jump").cast("int").alias("broad_jump"),
            F.col("cone").cast("float").alias("cone"),
            F.col("shuttle").cast("float").alias("shuttle"),
            F.col("ht").cast("string").alias("height"),
            F.col("wt").cast("string").alias("weight"),
        )
    )
