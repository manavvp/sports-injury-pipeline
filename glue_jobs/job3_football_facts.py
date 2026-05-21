"""
Job 3 - Football Facts builder (injury events + performance metrics).

Builds three football-specific fact tables:
  - fact_injury_event (Football side)
    Player injury records are collapsed into discrete events. Same injury
    across multiple consecutive reports → single event with start/end dates.
  - fact_football_performance_seasonal (per-season stats by competition)
  - fact_football_market_value (market value time series)

Pattern: GlueContext reads, PySpark transforms, Parquet writes.
Injury collapsing uses the "island and gap" pattern to group consecutive reports.
"""
import logging
import sys
from typing import Iterable

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

LOG = logging.getLogger("job3_football_facts")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

S3_BUCKET = "sports-injury-pipeline-manav"
PROCESSED_PREFIX = f"s3://{S3_BUCKET}/processed"
RAW_DB = "sports_injury_raw"
PROCESSED_DB = "sports_injury_processed"


def read_catalog(glue_context: GlueContext, table_name: str) -> DataFrame:
    dyf = glue_context.create_dynamic_frame.from_catalog(
        database=RAW_DB,
        table_name=table_name,
        transformation_ctx=f"src_{table_name}",
    )
    return dyf.toDF()


def read_processed_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a processed-layer Parquet table from S3."""
    path = f"{PROCESSED_PREFIX}/{table_name}/"
    return spark.read.parquet(path)


def write_parquet(df: DataFrame, table_name: str, partition_by: str = None) -> None:
    path = f"{PROCESSED_PREFIX}/{table_name}/"
    row_count = df.count()
    LOG.info("Writing %s rows to %s", row_count, path)
    writer = df.coalesce(1).write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(path)


def build_fact_injury_event_football(
    glue_context: GlueContext, spark: SparkSession
) -> DataFrame:
    """
    Football injury events from player injury records.

    football_player_injuries has one row per player per injury report with injury status.
    The same injury (e.g., "Hamstring") appears across multiple consecutive reports.
    Collapse consecutive reports into discrete injury events with start/end dates.

    Island-and-gap pattern:
    1. Filter to sports injuries only (join to dim_injury_type, keep is_sports_injury=true)
    2. Sort by (player_id, injury_type, date)
    3. Detect gaps: if current date is >14 days after previous for same player+injury,
       it's a new injury event (new island)
    4. GROUP BY island → one row per injury event with start/end/duration
    """
    raw_injuries = read_catalog(glue_context, "football_player_injuries")
    dim_injury_type = read_processed_table(spark, "dim_injury_type")
    dim_player = read_processed_table(spark, "dim_player")

    # Join to dim_injury_type to filter sports injuries only
    with_injury_type = raw_injuries.join(
        dim_injury_type.select("raw_injury_text", "injury_type_id", "is_sports_injury"),
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

    # Island and gap: mark the start of each new injury event
    w_order = Window.partitionBy("player_id", "injury_reason").orderBy("date_from")
    with_gap = (
        filtered
        .withColumn("_prev_date", F.lag("date_from").over(w_order))
        # Gap = first row (prev_date is NULL) OR more than 14 days since last report
        .withColumn(
            "_is_gap",
            F.col("_prev_date").isNull()
            | (F.datediff(F.col("date_from"), F.col("_prev_date")) > 14),
        )
        # Island ID = cumulative count of gaps
        .withColumn(
            "_island_id",
            F.sum(F.when(F.col("_is_gap"), 1).otherwise(0)).over(w_order),
        )
    )

    # Collapse each island into one injury event
    collapsed = (
        with_gap
        .groupBy("player_id", "injury_reason", "injury_type_id", "_island_id")
        .agg(
            F.min("date_from").alias("injury_date"),
            F.max(F.coalesce(F.col("date_until"), F.col("date_from"))).alias("return_date"),
            # Attribute span-crossing injuries to the season they started in
            F.min("season_name").alias("season_name"),
            # Games missed = count of records (each record = match/game availability report)
            F.count("*").alias("games_missed"),
        )
        .withColumn("days_missed", F.datediff(F.col("return_date"), F.col("injury_date")))
    )

    # Join to dim_player to get surrogate player_id
    with_player = collapsed.join(
        dim_player.filter(F.col("sport") == F.lit("football")).select(
            F.col("player_id").alias("player_id_surrogate"),
            F.col("source_player_id").alias("player_id"),
        ),
        on="player_id",
        how="inner",
    )

    # Assign injury event IDs (deterministic: ordered by player, injury, date)
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
    glue_context: GlueContext, spark: SparkSession
) -> DataFrame:
    """
    Football player performance stats. One row per player per season/competition.
    Joins player_performances to dim_player for FK to surrogate player_id.
    """
    raw_performance = read_catalog(glue_context, "football_player_performances")
    dim_player = read_processed_table(spark, "dim_player")

    football_players = dim_player.filter(F.col("sport") == F.lit("football")).select(
        F.col("player_id"), F.col("source_player_id").alias("player_id_raw")
    )

    # Rename raw side's player_id to avoid collision with surrogate from dim_player
    perf_renamed = raw_performance.withColumnRenamed("player_id", "src_player_id")
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
    glue_context: GlueContext, spark: SparkSession
) -> DataFrame:
    """
    Football player market value time series. One row per player per date.
    Converts Unix timestamp to proper date. Value is in euros.
    """
    raw_market = read_catalog(glue_context, "football_player_market_value")
    dim_player = read_processed_table(spark, "dim_player")

    football_players = dim_player.filter(F.col("sport") == F.lit("football")).select(
        F.col("player_id"), F.col("source_player_id").alias("player_id_raw")
    )

    market_renamed = raw_market.withColumnRenamed("player_id", "src_player_id")
    joined = market_renamed.join(
        football_players,
        market_renamed["src_player_id"] == football_players["player_id_raw"],
        how="inner",
    )

    return (
        joined
        .select(
            F.col("player_id"),
            # Convert Unix timestamp (seconds) to date
            F.from_unixtime(F.col("date_unix"), "yyyy-MM-dd").cast("date").alias("date"),
            F.col("value").cast("int").alias("market_value"),
        )
    )


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    # Dynamic partition overwrite: overwrite only the partitions this job writes,
    # not the entire table prefix. Lets Job 2 and Job 3 share fact_injury_event/
    # without one wiping the other's data.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    try:
        LOG.info("Building fact_injury_event (Football)")
        fact_injury_football = build_fact_injury_event_football(glue_context, spark)
        write_parquet(fact_injury_football, "fact_injury_event", partition_by="sport")

        LOG.info("Building fact_football_performance_seasonal")
        fact_perf = build_fact_football_performance_seasonal(glue_context, spark)
        write_parquet(fact_perf, "fact_football_performance_seasonal")

        LOG.info("Building fact_football_market_value")
        fact_market = build_fact_football_market_value(glue_context, spark)
        write_parquet(fact_market, "fact_football_market_value")
    except Exception:
        LOG.exception("Job 3 failed")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
