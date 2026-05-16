# Sports Injury Pipeline ⚡

An end-to-end data engineering pipeline on AWS to analyse if: ** athletes return to pre-injury performance levels?**

The pipeline ingests, models, and serves injury and performance data across two sports — NFL and association football (via Transfermarkt) — using a constellation schema that enables cross-sport analysis through conformed dimensions.

Built as a portfolio project out of curiosity to understand on how impactful major injuries are on players' careers.

---

## Architecture

```
                          ┌─────────────────────────────────┐
                          │           Raw Layer              │
  nflverse (public)  ───► │  S3: sports-injury-pipeline-     │
  salimt/football-   ───► │  manav/raw/                      │
  datasets (scraped) ───► │  ├── nfl/injuries/               │
                          │  ├── nfl/player_stats/           │
                          │  ├── nfl/rosters/                │
                          │  ├── nfl/combine/                │
                          │  └── football/player_*/          │
                          └────────────┬────────────────────┘
                                       │
                               Glue Data Catalog
                               (sports_injury_raw)
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                   │
             Job 1: Dimensions  Job 2: NFL Facts   Job 3: Football Facts
             (dim_injury_type,  (fact_injury_event  (fact_injury_event
              dim_player,        NFL, weekly +       football,
              dim_nfl_combine)   seasonal perf)      perf + market value)
                    │                  │                   │
                    └──────────────────┴──────────────────┘
                                       │
                          ┌────────────▼────────────────────┐
                          │        Processed Layer           │
                          │  S3: .../processed/              │
                          │  Parquet, Hive-partitioned       │
                          │  where applicable                │
                          └────────────┬────────────────────┘
                                       │
                               Glue Data Catalog
                               (sports_injury_processed)
                                       │
                                    Athena
                             (ad-hoc + analysis queries)
```

---

## Data Sources

### NFL — nflverse (public)

Weekly injury reports, player stats, rosters, and combine measurements for 2022–2023 seasons. All downloaded directly from the nflverse GitHub releases.

```bash
# Injuries
curl -L https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_2022.csv -o injuries_2022.csv
curl -L https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_2023.csv -o injuries_2023.csv

# Player stats
curl -L https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_2022.csv -o player_stats_2022.csv
curl -L https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_2023.csv -o player_stats_2023.csv

# Rosters
curl -L https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_2022.csv -o roster_2022.csv
curl -L https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_2023.csv -o roster_2023.csv

# Combine
curl -L https://github.com/nflverse/nflverse-data/releases/download/combine/combine.csv -o combine.csv
```

### Football / Soccer — salimt/football-datasets

Raw scraped Transfermarkt data: 93K+ players across player profiles, injury histories, performance stats, and market values. Download from the [salimt/football-datasets](https://github.com/salimt/football-datasets) repository and upload to S3 under `raw/football/`.

`first_dataset_upload.py` contains the bootstrap script used to push NFL data to S3 on initial setup.

---

## Processed Layer — Data Model

The processed layer uses a **constellation schema** (multi-fact star): multiple fact tables sharing conformed dimensions. Performance metrics differ fundamentally by sport, so keeping them in separate fact tables avoids a wide sparse anti-pattern.

```
dim_player          ◄──────────────────────────────────┐
dim_injury_type     ◄─────────────────────┐            │
dim_nfl_combine     ◄── (NFL only, FK to dim_player)   │
                                          │            │
                              fact_injury_event        │
                              (partitioned by sport)   │
                                                       │
                    fact_nfl_performance_weekly    ─────┘
                    fact_nfl_performance_seasonal  ─────┘
                    fact_football_performance      ─────┘
                    fact_football_market_value     ─────┘
```

**3.05M rows across 8 raw tables.** Football data is ~80x larger than NFL — this asymmetry informed Glue job sizing and data partitioning choices.

---

## Repository Structure

```
.
├── glue_jobs/
│   ├── job1_dimensions.py          # dim_injury_type, dim_player, dim_nfl_combine
│   ├── job2_nfl_facts.py           # NFL side of fact tables
│   ├── job3_football_facts.py      # Football side of fact tables
│   ├── deploy_job1.sh              # Upload + create/update Glue job + run
│   ├── deploy_job2.sh
│   ├── deploy_job3.sh
│   └── catalog/
│       └── register_tables.py      # Schema-as-code: upserts all processed-layer tables
│                                   # into the Glue catalog via boto3
├── reference/
│   └── injury_type_lookup.csv      # 432 injury mappings (body_region / injury_category /
│                                   # severity) across NFL and football. Business logic
│                                   # lives here, not in the Glue scripts.
├── infra/
│   ├── glue-s3-policy.json         # IAM policy for Glue S3 access
│   ├── glue-trust-policy.json      # Glue trust relationship
│   └── *.json                      # Glue table definition templates
└── first_dataset_upload.py         # Bootstrap: downloads nflverse CSVs and uploads to S3
```

---

## Running the Pipeline

### Prerequisites

- AWS CLI configured with a profile that has S3, Glue, IAM read permissions
- S3 bucket created: `sports-injury-pipeline-manav`
- Glue databases created: `sports_injury_raw`, `sports_injury_processed`
- IAM role `GlueServiceRole` with S3 read/write and Glue catalog access
- Python 3.x with `boto3` installed (for `register_tables.py`)

### Step 1 — Upload raw data to S3

Download the nflverse files using the curl commands above, then run:

```bash
python first_dataset_upload.py
```

Upload football-datasets CSVs to `s3://sports-injury-pipeline-manav/raw/football/` manually or via the AWS CLI.

### Step 2 — Crawl the raw layer

Run the two raw-layer crawlers (`nfl-raw-crawler`, `football-raw-crawler`) to populate `sports_injury_raw` in the Glue catalog:

```bash
aws glue start-crawler --name nfl-raw-crawler
aws glue start-crawler --name football-raw-crawler
```

### Step 3 — Deploy and run the Glue jobs in order

Jobs must run in dependency order. Jobs 2 and 3 can run in parallel after Job 1 completes.

```bash
# Job 1 — dimensions (no dependencies)
cd glue_jobs
./deploy_job1.sh all

# Job 2 and 3 — facts (depend on Job 1)
./deploy_job2.sh all
./deploy_job3.sh all
```

Each deploy script handles: upload the Python script to S3, create-or-update the Glue job definition, start a run. Re-running any job is safe — all writes use overwrite mode with dynamic partition isolation.

### Step 4 — Register the processed layer

```bash
# Upsert all 8 processed-layer table definitions into the Glue catalog
python glue_jobs/catalog/register_tables.py

# Register partitions on fact_injury_event (written by both Job 2 and Job 3)
# Run in Athena:
MSCK REPAIR TABLE sports_injury_processed.fact_injury_event;
```

### Step 5 — Validate in Athena

```sql
-- Row counts across all processed tables
SELECT 'dim_player'                  AS t, COUNT(*) FROM sports_injury_processed.dim_player
UNION ALL SELECT 'dim_injury_type',         COUNT(*) FROM sports_injury_processed.dim_injury_type
UNION ALL SELECT 'dim_nfl_combine',         COUNT(*) FROM sports_injury_processed.dim_nfl_combine
UNION ALL SELECT 'fact_injury_event',       COUNT(*) FROM sports_injury_processed.fact_injury_event
UNION ALL SELECT 'fact_nfl_performance_weekly',   COUNT(*) FROM sports_injury_processed.fact_nfl_performance_weekly
UNION ALL SELECT 'fact_nfl_performance_seasonal', COUNT(*) FROM sports_injury_processed.fact_nfl_performance_seasonal
UNION ALL SELECT 'fact_football_performance',     COUNT(*) FROM sports_injury_processed.fact_football_performance
UNION ALL SELECT 'fact_football_market_value',    COUNT(*) FROM sports_injury_processed.fact_football_market_value;

-- Partitions registered correctly
SELECT sport, COUNT(*) AS injury_events
FROM sports_injury_processed.fact_injury_event
GROUP BY sport;

-- FK integrity check — should return 0
SELECT COUNT(*) FROM sports_injury_processed.fact_injury_event f
LEFT JOIN sports_injury_processed.dim_player p USING (player_id)
WHERE p.player_id IS NULL;
```

---

## Key Design Decisions

**Hybrid GlueContext + PySpark pattern.** GlueContext reads from the Glue Data Catalog (respects the schema contract), PySpark DataFrames for all transformation logic (window functions, aggregations — DynamicFrame's API can't do these), and plain `.write.parquet()` for S3 output. The transformation layer is portable to Databricks or EMR without rewrite.

**Externalized injury classification.** 432 injury-type mappings live in `reference/injury_type_lookup.csv`, not in the Glue scripts. Business logic changes (a new injury category, a severity reclassification) don't require redeployment. New unseen injury strings produce NULL classifications via left join — nothing breaks, a monitoring query surfaces gaps.

**Schema-as-code for the processed layer.** `register_tables.py` declares all column names, types, and partition keys explicitly via boto3. This over a Glue crawler: inference is a discovery tool for unknown schemas; the pipeline authors the output, so the schema is already known. Inferred types (bigint vs int, struct vs string) introduce catalog drift.

**Dynamic partition overwrite.** Jobs 2 and 3 share the `fact_injury_event/` S3 prefix, writing to `sport=nfl/` and `sport=football/` respectively. Setting `spark.sql.sources.partitionOverwriteMode = dynamic` scopes each overwrite to only the partitions that job produces — full idempotency, no cross-job collision.

**NFL injury event collapsing.** The `nfl_injuries` source is weekly report observations, not discrete events. The pipeline uses an island-and-gap pattern (PySpark window functions) to reconstruct events: same player + same injury within a 14-day window = one event. Duration is floored at `games_missed * 7` to avoid zero-duration events for single-week islands.

**NULL over inferred values.** NFL source data doesn't provide player nationality. The field is NULL — not defaulted to "USA". NULLs are auditable; fabricated data that's mostly right is silently wrong.

---

## AWS Stack

| Service | Role |
|---|---|
| S3 | Raw and processed data storage |
| Glue Data Catalog | Schema registry for both layers |
| Glue ETL (PySpark) | Three transformation jobs |
| Athena | Ad-hoc SQL over processed Parquet |
| IAM | `GlueServiceRole` with scoped S3 + catalog permissions |

**Intentional scope exclusions:** no CI/CD (the deploy scripts are the primitive that CI would call), no S3 lifecycle policies, no MWAA orchestration (cost decision for a portfolio project). All acknowledged, all defensible.

---

## AWS CLI Workflow

Everything in this project is CLI-only — no console clicks. The deploy scripts, `register_tables.py`, and the raw crawler commands above are the complete operational surface.

---

## License

MIT
