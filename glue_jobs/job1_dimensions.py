"""
Job 1 - Dimension builder for the Sports Injury Pipeline.

Builds three conformed dimensions from the raw Glue Catalog and writes
them as Parquet under s3://sports-injury-pipeline-manav/processed/:

  - dim_injury_type   (from reference/injury_type_lookup.csv)
  - dim_player        (NFL rosters + football profiles, conformed)
  - dim_nfl_combine   (NFL combine, FK'd to dim_player via pfr_id -> gsis_id)

Pattern: GlueContext for catalog reads (DynamicFrame -> toDF), pure PySpark
transforms in glue_jobs/transforms/, PySpark .write.parquet() for output.
"""
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame

from transforms.dimensions import (
    build_dim_injury_type,
    build_dim_nfl_combine,
    build_dim_player,
)

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


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    try:
        LOG.info("Building dim_injury_type")
        injury_lookup_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(LOOKUP_PATH)
        )
        dim_injury_type = build_dim_injury_type(injury_lookup_df)
        write_parquet(dim_injury_type, "dim_injury_type")

        # Read rosters once — used by both dim_player and dim_nfl_combine.
        rosters_df = read_catalog(glue_context, "nfl_rosters")

        LOG.info("Building dim_player")
        profiles_df = read_catalog(glue_context, "football_player_profiles")
        dim_player = build_dim_player(rosters_df, profiles_df).cache()
        write_parquet(dim_player, "dim_player")

        LOG.info("Building dim_nfl_combine")
        combine_df = read_catalog(glue_context, "nfl_combine")
        dim_nfl_combine = build_dim_nfl_combine(combine_df, rosters_df, dim_player)
        write_parquet(dim_nfl_combine, "dim_nfl_combine")
    except Exception:
        LOG.exception("Job 1 failed")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
