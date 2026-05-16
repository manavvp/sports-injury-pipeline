"""
Job 1 - Dimension builder for the Sports Injury Pipeline.

Builds three conformed dimensions from the raw Glue Catalog and writes
them as Parquet under s3://sports-injury-pipeline-manav/processed/:

  - dim_injury_type   (from reference/injury_type_lookup.csv)
  - dim_player        (NFL rosters + football profiles, conformed)
  - dim_nfl_combine   (NFL combine, FK'd to dim_player via pfr_id -> gsis_id)

Pattern: GlueContext for catalog reads (DynamicFrame -> toDF), PySpark
DataFrame for transforms, PySpark .write.parquet() for output.
"""
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

LOG = logging.getLogger("job1_dimensions")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

S3_BUCKET = "sports-injury-pipeline-manav"
PROCESSED_PREFIX = f"s3://{S3_BUCKET}/processed"
RAW_DB = "sports_injury_raw"
LOOKUP_PATH = f"s3://{S3_BUCKET}/reference/injury_type_lookup.csv"


def read_catalog(glue_context: GlueContext, table_name: str) -> DataFrame:
    """Read a raw Glue Catalog table as a Spark DataFrame."""
    dyf = glue_context.create_dynamic_frame.from_catalog(
        database=RAW_DB,
        table_name=table_name,
        transformation_ctx=f"src_{table_name}",
    )
    return dyf.toDF()


def write_parquet(df: DataFrame, table_name: str) -> None:
    """Idempotent overwrite to processed/<table>/. Coalesce small dims to one file."""
    path = f"{PROCESSED_PREFIX}/{table_name}/"
    row_count = df.count()
    LOG.info("Writing %s rows to %s", row_count, path)
    df.coalesce(1).write.mode("overwrite").parquet(path)


def build_dim_injury_type(spark: SparkSession) -> DataFrame:
    """
    Lookup CSV is the source of truth for injury classification - never hardcode
    these mappings in the job. Surrogate key ordered by (sport, raw_injury_text)
    so reruns produce stable ids across NFL/football fact joins.
    """
    raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(LOOKUP_PATH)
    )

    typed = raw.select(
        F.col("sport").cast("string").alias("sport"),
        F.col("raw_injury_text").cast("string").alias("raw_injury_text"),
        F.col("body_region").cast("string").alias("body_region"),
        F.col("injury_category").cast("string").alias("injury_category"),
        F.col("severity").cast("string").alias("severity"),
        # CSV stores "true"/"false" as text - convert to actual boolean
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


def build_dim_player(glue_context: GlueContext) -> DataFrame:
    """
    Conformed player dim across sports.

    NFL rosters carry one row per player per (season, week) - reduce to the
    latest snapshot per gsis_id so the dim reflects most recent metadata.
    Football profiles are already keyed by player_id; dedupe defensively.

    Nationality is NULL for NFL (the source doesn't carry it - never infer).
    Weight and college are NULL for football (not in Transfermarkt profiles).
    """
    rosters = read_catalog(glue_context, "nfl_rosters")
    profiles = read_catalog(glue_context, "football_player_profiles")

    latest_roster = Window.partitionBy("gsis_id").orderBy(
        F.desc("season"), F.desc("week")
    )
    nfl = (
        rosters
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
        profiles
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
    glue_context: GlueContext, dim_player: DataFrame
) -> DataFrame:
    """
    Combine -> dim_player FK chain: nfl_combine.pfr_id -> nfl_rosters.pfr_id
    -> nfl_rosters.gsis_id -> dim_player.source_player_id (sport='nfl').

    Inner joins drop combine participants who never made an NFL roster. That
    is expected - dim_nfl_combine only describes athletes who reached the league.
    Natural key is (player_id, season); no surrogate needed.
    """
    combine = read_catalog(glue_context, "nfl_combine")
    rosters = read_catalog(glue_context, "nfl_rosters")

    pfr_to_gsis = (
        rosters
        .filter(F.col("pfr_id").isNotNull() & F.col("gsis_id").isNotNull())
        .select("pfr_id", "gsis_id")
        .dropDuplicates(["pfr_id"])
    )

    nfl_players = (
        dim_player
        .filter(F.col("sport") == F.lit("nfl"))
        .select(
            F.col("player_id"),
            F.col("source_player_id").alias("gsis_id"),
        )
    )

    return (
        combine
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


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    try:
        LOG.info("Building dim_injury_type")
        dim_injury_type = build_dim_injury_type(spark)
        write_parquet(dim_injury_type, "dim_injury_type")

        LOG.info("Building dim_player")
        dim_player = build_dim_player(glue_context).cache()
        write_parquet(dim_player, "dim_player")

        LOG.info("Building dim_nfl_combine")
        dim_nfl_combine = build_dim_nfl_combine(glue_context, dim_player)
        write_parquet(dim_nfl_combine, "dim_nfl_combine")
    except Exception:
        LOG.exception("Job 1 failed")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
