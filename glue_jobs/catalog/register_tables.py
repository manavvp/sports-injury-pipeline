"""
Register processed-layer tables in the Glue Catalog.

Schema-as-data: every processed table is defined once in TABLES, then
upserted via boto3. As Job 2 and Job 3 land, append entries here - the
build/upsert path is reused.

Run modes:
    python register_tables.py                  # upsert all known tables
    python register_tables.py dim_player ...   # upsert a subset
"""
import logging
import sys
from typing import Iterable, Sequence

import boto3
from botocore.exceptions import ClientError

LOG = logging.getLogger("register_tables")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

DB = "sports_injury_processed"
BUCKET = "sports-injury-pipeline-manav"
PROCESSED_PREFIX = f"s3://{BUCKET}/processed"

PARQUET_INPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"


# Single source of truth for every processed-layer table schema.
# Each value is a dict with 'columns' (sequence of (name, type) tuples) and optional 'partitions'.
# Tables from all three jobs. Call selectively after each job completes:
#   python register_tables.py dim_injury_type dim_player dim_nfl_combine  (after Job 1)
#   python register_tables.py fact_injury_event fact_nfl_performance_weekly ...  (after Job 2)
TABLES: dict[str, dict] = {
    # ===== JOB 1: DIMENSIONS =====
    "dim_injury_type": {
        "columns": (
            ("injury_type_id", "int"),
            ("body_region", "string"),
            ("injury_category", "string"),
            ("severity", "string"),
            ("raw_injury_text", "string"),
            ("sport", "string"),
            ("is_sports_injury", "boolean"),
        ),
    },
    "dim_player": {
        "columns": (
            ("player_id", "int"),
            ("source_player_id", "string"),
            ("sport", "string"),
            ("player_name", "string"),
            ("date_of_birth", "date"),
            ("position", "string"),
            ("height", "string"),
            ("weight", "string"),
            ("nationality", "string"),
            ("college", "string"),
        ),
    },
    "dim_nfl_combine": {
        "columns": (
            ("player_id", "int"),
            ("season", "int"),
            ("pos", "string"),
            ("school", "string"),
            ("forty", "float"),
            ("bench", "int"),
            ("vertical", "float"),
            ("broad_jump", "int"),
            ("cone", "float"),
            ("shuttle", "float"),
            ("height", "string"),
            ("weight", "string"),
        ),
    },
    # ===== JOB 2: NFL FACTS =====
    "fact_injury_event": {
        "columns": (
            ("injury_event_id", "int"),
            ("player_id", "int"),
            ("injury_type_id", "int"),
            ("season", "string"),
            ("injury_date", "date"),
            ("return_date", "date"),
            ("days_missed", "int"),
            ("games_missed", "int"),
        ),
        "partitions": (
            ("sport", "string"),
        ),
    },
    "fact_nfl_performance_weekly": {
        "columns": (
            ("player_id", "int"),
            ("season", "int"),
            ("week", "int"),
            ("season_type", "string"),
            ("opponent_team", "string"),
            ("completions", "int"),
            ("attempts", "int"),
            ("passing_yards", "int"),
            ("passing_tds", "int"),
            ("interceptions", "int"),
            ("sacks", "float"),
            ("carries", "int"),
            ("rushing_yards", "int"),
            ("rushing_tds", "int"),
            ("receptions", "int"),
            ("targets", "int"),
            ("receiving_yards", "int"),
            ("receiving_tds", "int"),
            ("passing_epa", "float"),
            ("rushing_epa", "float"),
            ("receiving_epa", "float"),
            ("target_share", "float"),
            ("fantasy_points", "float"),
            ("fantasy_points_ppr", "float"),
        ),
    },
    "fact_nfl_performance_seasonal": {
        "columns": (
            ("player_id", "int"),
            ("season", "int"),
            ("games_played", "int"),
            ("total_completions", "int"),
            ("total_attempts", "int"),
            ("total_passing_yards", "int"),
            ("total_passing_tds", "int"),
            ("total_interceptions", "int"),
            ("total_carries", "int"),
            ("total_rushing_yards", "int"),
            ("total_rushing_tds", "int"),
            ("total_receptions", "int"),
            ("total_targets", "int"),
            ("total_receiving_yards", "int"),
            ("total_receiving_tds", "int"),
            ("avg_passing_epa", "float"),
            ("avg_rushing_epa", "float"),
            ("avg_receiving_epa", "float"),
            ("total_fantasy_points", "float"),
            ("total_fantasy_points_ppr", "float"),
        ),
    },
    # ===== JOB 3: FOOTBALL FACTS =====
    "fact_football_performance_seasonal": {
        "columns": (
            ("player_id", "int"),
            ("season_name", "string"),
            ("competition_name", "string"),
            ("team_name", "string"),
            ("goals", "int"),
            ("assists", "int"),
            ("minutes_played", "int"),
            ("games_played", "int"),
            ("yellow_cards", "int"),
            ("red_cards", "int"),
            ("clean_sheets", "int"),
            ("goals_conceded", "int"),
        ),
    },
    "fact_football_market_value": {
        "columns": (
            ("player_id", "int"),
            ("date", "date"),
            ("market_value", "int"),
        ),
    },
}


def build_table_input(
    name: str, columns: Iterable[tuple[str, str]], partitions: Iterable[tuple[str, str]] = None
) -> dict:
    """Construct the Glue TableInput payload for an external Parquet table."""
    table_input = {
        "Name": name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
        "StorageDescriptor": {
            "Columns": [{"Name": c, "Type": t} for c, t in columns],
            "Location": f"{PROCESSED_PREFIX}/{name}/",
            "InputFormat": PARQUET_INPUT,
            "OutputFormat": PARQUET_OUTPUT,
            "SerdeInfo": {"SerializationLibrary": PARQUET_SERDE},
        },
    }
    if partitions:
        table_input["PartitionKeys"] = [{"Name": c, "Type": t} for c, t in partitions]
    return table_input


def upsert_table(glue, db: str, name: str, table_spec: dict) -> None:
    """
    Update the table if it exists, otherwise create it. Mirrors the
    create-or-update pattern in deploy_job1.sh so re-runs are always safe.
    """
    columns = table_spec.get("columns", ())
    partitions = table_spec.get("partitions", None)
    table_input = build_table_input(name, columns, partitions)
    try:
        glue.update_table(DatabaseName=db, TableInput=table_input)
        LOG.info("Updated %s.%s", db, name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityNotFoundException":
            glue.create_table(DatabaseName=db, TableInput=table_input)
            LOG.info("Created %s.%s", db, name)
        else:
            raise


def main(table_names: Sequence[str] = ()) -> None:
    glue = boto3.client("glue")
    targets = list(table_names) or list(TABLES.keys())
    unknown = set(targets) - set(TABLES.keys())
    if unknown:
        raise KeyError(f"Unknown tables: {sorted(unknown)}")
    for name in targets:
        upsert_table(glue, DB, name, TABLES[name])
    LOG.info("Done. Registered: %s", sorted(targets))


if __name__ == "__main__":
    main(sys.argv[1:])
