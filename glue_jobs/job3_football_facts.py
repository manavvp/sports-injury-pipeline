"""
Job 3 - Football Facts builder (injury events + performance metrics).

Builds three football-specific fact tables:
  - fact_injury_event (Football side)
  - fact_football_performance_seasonal
  - fact_football_market_value

Pattern: GlueContext reads, pure PySpark transforms in glue_jobs/transforms/,
Parquet writes.
"""
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession

from transforms.football_facts import (
    build_fact_football_market_value,
    build_fact_football_performance_seasonal,
    build_fact_injury_event_football,
)

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
        # Processed-layer dims — used across multiple transforms.
        dim_player_df = read_processed_table(spark, "dim_player")
        dim_injury_type_df = read_processed_table(spark, "dim_injury_type")

        LOG.info("Building fact_injury_event (Football)")
        raw_injuries_df = read_catalog(glue_context, "football_player_injuries")
        fact_injury_football = build_fact_injury_event_football(
            raw_injuries_df, dim_injury_type_df, dim_player_df
        )
        write_parquet(fact_injury_football, "fact_injury_event", partition_by="sport")

        LOG.info("Building fact_football_performance_seasonal")
        raw_performance_df = read_catalog(glue_context, "football_player_performances")
        fact_perf = build_fact_football_performance_seasonal(
            raw_performance_df, dim_player_df
        )
        write_parquet(fact_perf, "fact_football_performance_seasonal")

        LOG.info("Building fact_football_market_value")
        raw_market_df = read_catalog(glue_context, "football_player_market_value")
        fact_market = build_fact_football_market_value(raw_market_df, dim_player_df)
        write_parquet(fact_market, "fact_football_market_value")
    except Exception:
        LOG.exception("Job 3 failed")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
