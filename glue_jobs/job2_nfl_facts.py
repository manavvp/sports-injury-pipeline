"""
Job 2 - NFL Facts builder (injury events + performance metrics).

Builds three NFL-specific fact tables:
  - fact_injury_event (NFL side)
    Weekly injury reports are collapsed into discrete events. Same injury
    across multiple consecutive weeks → single event with start/end dates.
  - fact_nfl_performance_weekly (granular stats)
  - fact_nfl_performance_seasonal (aggregated stats)

Pattern: GlueContext reads, PySpark transforms, Parquet writes.
Injury collapsing uses the "island and gap" pattern to group consecutive weeks.
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

LOG = logging.getLogger("job2_nfl_facts")
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


def build_fact_injury_event_nfl(
    glue_context: GlueContext, spark: SparkSession
) -> DataFrame:
    """
    NFL injury events from weekly injury reports.

    nfl_injuries has one row per player per week with injury status. The same
    injury (e.g., "Hamstring") appears across multiple weeks. We collapse those
    weekly observations into discrete injury events with start/end dates.

    Two judgment calls baked in (both defensible, both tunable):

    1. GAP_DAYS = 14. Two reports for the same player+injury are part of the
       same event if they are within 14 days of each other. Matches the football
       collapsing logic in Job 3 for symmetry. A wider window merges genuine
       re-injuries; a narrower one fragments single events.

    2. Duration floor = games_missed * 7. NFL reports give us observation dates
       (date_modified), not injury spans. A single-row island has min == max,
       so naive datediff = 0 even if the player was held Out for that week.
       Flooring at games_missed * 7 attributes at least one week of duration
       per missed game. Events where the player was never Out/Doubtful keep
       days_missed = 0 — they represent Questionable/Probable reports where
       the player played through, and analytical queries can filter them out.

    Island-and-gap implementation: lag(date_modified) over the partition, flag
    a "new island" when gap > GAP_DAYS, cumulative-sum the flags to assign an
    island ID, then groupBy island ID.
    """
    GAP_DAYS = 14
    raw_injuries = read_catalog(glue_context, "nfl_injuries")
    dim_injury_type = read_processed_table(spark, "dim_injury_type")
    dim_player = read_processed_table(spark, "dim_player")

    # Join to dim_injury_type to filter sports injuries only
    with_injury_type = raw_injuries.join(
        dim_injury_type.select("raw_injury_text", "injury_type_id", "is_sports_injury"),
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

    # Island and gap: mark the start of each new injury event
    w_order = Window.partitionBy("gsis_id", "report_primary_injury").orderBy(
        "date_modified"
    )
    with_gap = (
        filtered
        .withColumn("_prev_date", F.lag("date_modified").over(w_order))
        .withColumn(
            "_is_gap",
            F.col("_prev_date").isNull()
            | (F.datediff(F.col("date_modified"), F.col("_prev_date")) > GAP_DAYS),
        )
        .withColumn(
            "_island_id",
            F.sum(F.when(F.col("_is_gap"), 1).otherwise(0)).over(w_order),
        )
    )

    # Collapse each island into one injury event
    collapsed = (
        with_gap
        .groupBy("gsis_id", "report_primary_injury", "injury_type_id", "_island_id")
        .agg(
            F.min("date_modified").alias("injury_date"),
            F.max("date_modified").alias("return_date"),
            # Attribute span-crossing injuries to the season they started in
            F.min("season").alias("season"),
            # Games missed = weeks where status is Out or Doubtful (not Questionable/Probable)
            F.sum(
                F.when(
                    F.col("report_status").isin(F.lit("Out"), F.lit("Doubtful")), 1
                ).otherwise(0)
            ).cast("int").alias("games_missed"),
        )
        # Duration floor: at least 7 days per game missed. Single-week islands
        # otherwise produce days_missed = 0 by construction (min == max).
        .withColumn(
            "days_missed",
            F.greatest(
                F.datediff(F.col("return_date"), F.col("injury_date")),
                F.col("games_missed") * F.lit(7),
            ),
        )
    )

    # Join to dim_player to get surrogate player_id
    with_player = collapsed.join(
        dim_player.filter(F.col("sport") == F.lit("nfl")).select(
            F.col("player_id"), F.col("source_player_id").alias("gsis_id")
        ),
        on="gsis_id",
        how="inner",
    )

    # Assign injury event IDs (deterministic: ordered by player, injury, date)
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
    glue_context: GlueContext, spark: SparkSession
) -> DataFrame:
    """
    NFL player weekly performance stats. One row per player per week.
    Joins player_stats to dim_player for FK to surrogate player_id.
    Removes columns that belong in dim_player (headshot_url, position, etc.).
    """
    raw_stats = read_catalog(glue_context, "nfl_player_stats")
    dim_player = read_processed_table(spark, "dim_player")

    nfl_players = dim_player.filter(F.col("sport") == F.lit("nfl")).select(
        F.col("player_id"), F.col("source_player_id").alias("player_id_raw")
    )

    # raw_stats.player_id is the gsis_id; rename to avoid collision with the
    # surrogate player_id from dim_player after the join.
    stats_renamed = raw_stats.withColumnRenamed("player_id", "gsis_id_raw")
    joined = stats_renamed.join(
        nfl_players,
        stats_renamed["gsis_id_raw"] == nfl_players["player_id_raw"],
        how="inner",
    )

    return (
        joined
        .select(
            F.col("player_id"),  # FK to dim_player
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


def build_fact_nfl_performance_seasonal(
    weekly: DataFrame,
) -> DataFrame:
    """
    Aggregate weekly stats to seasonal. Totals for counting stats (yards, TDs),
    averages for rate stats (EPA, target share).
    """
    return (
        weekly
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
        LOG.info("Building fact_injury_event (NFL)")
        fact_injury_nfl = build_fact_injury_event_nfl(glue_context, spark)
        write_parquet(fact_injury_nfl, "fact_injury_event", partition_by="sport")

        LOG.info("Building fact_nfl_performance_weekly")
        fact_perf_weekly = build_fact_nfl_performance_weekly(glue_context, spark)
        fact_perf_weekly.cache()
        write_parquet(fact_perf_weekly, "fact_nfl_performance_weekly")

        LOG.info("Building fact_nfl_performance_seasonal")
        fact_perf_seasonal = build_fact_nfl_performance_seasonal(fact_perf_weekly)
        write_parquet(fact_perf_seasonal, "fact_nfl_performance_seasonal")
    except Exception:
        LOG.exception("Job 2 failed")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
