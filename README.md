# Sports Injury Pipeline - NFL, Football

An end-to-end data engineering pipeline on AWS to analyse if **athletes return to pre-injury performance levels.**

The pipeline ingests, models, and serves injury and performance data across two sports — NFL and association football (via Transfermarkt) — using a constellation schema that enables cross-sport analysis through conformed dimensions.

Built as a portfolio project out of curiosity to understand how impactful major injuries are on players' careers. Infrastructure is fully managed with Terraform, and both the infrastructure and the transformation code ship through GitHub Actions CI/CD.

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

All AWS resources in this diagram — buckets, Glue databases, jobs, crawlers, IAM roles, the OIDC provider — are provisioned by Terraform. See [Infrastructure as Code](#infrastructure-as-code-terraform).

---

## Data Sources

### NFL — nflverse (public)

Weekly injury reports, player stats, rosters, and combine measurements for the **2022–2023 seasons**. All downloaded directly from the nflverse GitHub releases.

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

# Combine (single aggregate file, all years)
curl -L https://github.com/nflverse/nflverse-data/releases/download/combine/combine.csv -o combine.csv
```

> **Upstream note — `player_stats` deprecation.** nflverse deprecated the `player_stats` release on 2025-08-01 in favour of a new `stats_player` tag (file pattern `stats_player_week_<year>.csv`) with a revised, consolidated schema. The `player_stats_<year>.csv` URLs above still resolve for 2022–2024 but 404 for 2025+. This project is pinned to the pre-deprecation `player_stats` files for its current 2022–2023 window; extending past 2024 requires migrating to the `stats_player` tag and remapping the changed columns in the Glue job.

### Football / Soccer — salimt/football-datasets

Raw scraped Transfermarkt data: 93K+ players across player profiles, injury histories, performance stats, and market values. Download from the [salimt/football-datasets](https://github.com/salimt/football-datasets) repository and upload to S3 under `raw/football/`.

> The soccer source is a one-time static dump (no incremental update mechanism), covering careers through roughly the 2024/25 season. The freshness asymmetry between the two sports — NFL updates weekly, soccer is frozen — is a real-world data engineering constraint rather than a defect, and is called out here deliberately.

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
                    fact_football_performance_seasonal ─────┘
                    fact_football_market_value     ─────┘
```

**3.05M rows across 8 raw tables.** Football data is ~80x larger than NFL — this asymmetry informed Glue job sizing and data partitioning choices.

Current processed-layer row counts (2022–2023 NFL window):

| Table | Rows |
|---|---|
| `dim_player` | 96,536 |
| `fact_injury_event` (sport=nfl) | 2,923 |
| `fact_injury_event` (sport=football) | 125,466 |
| `fact_nfl_performance_weekly` | 11,284 |
| `fact_football_market_value` | 901,429 |

---

## Repository Structure

```
.
├── glue_jobs/
│   ├── job1_dimensions.py          # Thin wrapper: reads catalog, calls transforms, writes S3
│   ├── job2_nfl_facts.py           # NFL side of the fact tables
│   ├── job3_football_facts.py      # Football side of the fact tables
│   ├── transforms/                 # Pure DataFrame→DataFrame logic, decoupled from Glue
│   │   ├── dimensions.py           #   — unit-testable with a local SparkSession, no AWS
│   │   ├── nfl_facts.py
│   │   └── football_facts.py
│   └── catalog/
│       └── register_tables.py      # Schema-as-code: upserts processed tables into the catalog
├── tests/                          # pytest suite over the transforms package
│   ├── conftest.py                 #   — session-scoped local SparkSession fixture
│   ├── test_dimensions.py
│   ├── test_nfl_facts.py
│   └── test_football_facts.py
├── terraform/                      # All AWS infrastructure (see IaC section)
│   ├── backend.tf                  # S3 remote state + native locking
│   ├── providers.tf                # AWS provider, default_tags
│   ├── s3.tf  glue.tf  iam.tf       # Buckets, Glue DBs/jobs/crawlers, roles
│   ├── github_oidc.tf              # OIDC provider + CI/CD assume-roles
│   └── catalog.tf
├── .github/workflows/
│   ├── ci.yml                      # PR: fmt/validate/plan + ruff/pytest
│   └── cd.yml                      # merge to main: apply + register + s3 sync
├── reference/
│   └── injury_type_lookup.csv      # 432 injury mappings (body_region / category / severity).
│                                   # Business logic lives here, not in the Glue scripts.
├── pyproject.toml                  # ruff + pytest config
├── requirements-dev.txt            # pyspark, pytest, ruff (local dev + CI)
└── first_dataset_upload.py         # Bootstrap: downloads nflverse CSVs and uploads to S3
```

---

## Infrastructure as Code (Terraform)

All AWS infrastructure is managed with Terraform in `terraform/`, adopted onto existing resources via `terraform import` rather than greenfield.

- **Flat module structure, logical file separation** (`s3.tf`, `glue.tf`, `iam.tf`, …). Modules are premature abstraction at this scale and are deliberately avoided.
- **Remote state:** S3 backend with native locking. The state bucket and lock table are an **unmanaged bootstrap** — created manually and documented as such, since a state backend can't manage its own existence.
- **Provider version pinned** in `required_providers`; `default_tags` (`Project`, `ManagedBy`) applied at the provider level so every resource inherits them.
- **IAM policies via `data "aws_iam_policy_document"`** blocks, not embedded JSON strings.

**In Terraform:** S3, Glue databases, Glue jobs, Glue crawlers, IAM roles/policies, CloudWatch log groups, the GitHub OIDC provider.

**Not in Terraform:** Glue *table schemas* (registered via `register_tables.py` — they evolve with the transformation code, not with infra deploys) and raw data files in S3.

---

## CI/CD (GitHub Actions)

Two workflows, split by concern and gated by path filters so a Terraform-only change doesn't run pytest and vice versa.

### `ci.yml` — on pull request
- **Terraform:** `fmt -check`, `validate`, `plan` — the plan is posted back to the PR as a comment so reviewers see the infra diff without running Terraform locally.
- **Python:** `ruff` (lint) and `pytest` on the extracted transformation functions.
- A red check blocks merge.

### `cd.yml` — on merge to main
- `terraform apply`
- `register_tables.py` (idempotent schema upsert into the Glue catalog)
- `aws s3 sync` of the Glue job scripts, the packaged `transforms.zip`, and the reference CSVs
- A deployment summary is written to the run's `$GITHUB_STEP_SUMMARY`.

### Authentication — OIDC, no long-lived keys
Both workflows authenticate to AWS via **OIDC federation**: GitHub mints a short-lived token, AWS verifies it and issues ~1-hour credentials. No static access keys live in GitHub Secrets. Trust policies scope by the GitHub `sub` claim so a **pull_request** can only assume the read-only **CI** role, and only a **push to main** can assume the write-capable **CD** role — the git event is bound to the AWS blast radius. Defined in `terraform/github_oidc.tf`.

---

## Development

The transformation logic is written as pure `DataFrame → DataFrame` functions in `glue_jobs/transforms/`, decoupled from Glue's runtime. This is what makes it unit-testable without AWS — the tests run a local `SparkSession` (see `tests/conftest.py`), feed small synthetic DataFrames, and assert on the output. The Glue jobs themselves are thin wrappers: read from catalog, call a transform, write to S3.

```bash
# One-time setup
pip install -r requirements-dev.txt

# Lint + test (the same commands CI runs)
ruff check .
pytest
```

`pytest` starts a local Spark session in-process (`local[2]`) — no Glue, no cluster, no credentials required.

---

## Running the Pipeline

### Prerequisites

- AWS CLI configured with credentials that can run Glue, read/write S3, and read the Glue catalog
- Infrastructure applied via Terraform (`cd terraform && terraform init && terraform apply`) — or let CD apply it on merge to main
- Python 3.x with `requirements-dev.txt` installed (for `register_tables.py` and local tests)

### Step 1 — Upload raw data to S3

Download the nflverse files using the curl commands above, then run:

```bash
python first_dataset_upload.py
```

Upload football-datasets CSVs to `s3://sports-injury-pipeline-manav/raw/football/` via the AWS CLI.

### Step 2 — Crawl the raw layer

Run the two raw-layer crawlers to populate `sports_injury_raw` in the Glue catalog:

```bash
aws glue start-crawler --name nfl-raw-crawler
aws glue start-crawler --name football-raw-crawler
```

### Step 3 — Run the Glue jobs in dependency order

Jobs 2 and 3 depend on Job 1 and can run in parallel once it completes. On merge to main, CD has already synced the latest job scripts to S3, so triggering a run is a single CLI call each:

```bash
# Job 1 — dimensions (no dependencies)
aws glue start-job-run --job-name job1_dimensions

# Jobs 2 & 3 — facts (after Job 1 succeeds; run in parallel)
aws glue start-job-run --job-name job2_nfl_facts
aws glue start-job-run --job-name job3_football_facts
```

All writes use overwrite mode with dynamic partition isolation, so re-running any job is safe.

### Step 4 — Register the processed layer

```bash
# Upsert all processed-layer table definitions into the Glue catalog
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
UNION ALL SELECT 'fact_football_performance_seasonal', COUNT(*) FROM sports_injury_processed.fact_football_performance_seasonal
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

**Transforms decoupled from the runtime.** Business logic lives in pure `DataFrame → DataFrame` functions under `glue_jobs/transforms/`, with the Glue jobs reduced to thin read/transform/write wrappers. This makes the logic unit-testable with a local SparkSession (no Glue, no AWS), keeps it lazy (the functions build query plans; the action stays in the wrapper, so Catalyst optimises across the whole chain), and portable across Spark runtimes.

**Externalized injury classification.** 432 injury-type mappings live in `reference/injury_type_lookup.csv`, not in the Glue scripts. Business logic changes (a new injury category, a severity reclassification) don't require redeployment. New unseen injury strings produce NULL classifications via left join — nothing breaks, a monitoring query surfaces gaps.

**Schema-as-code for the processed layer.** `register_tables.py` declares all column names, types, and partition keys explicitly via boto3. This over a Glue crawler: inference is a discovery tool for unknown schemas; the pipeline authors the output, so the schema is already known. Inferred types (bigint vs int, struct vs string) introduce catalog drift.

**Dynamic partition overwrite.** Jobs 2 and 3 share the `fact_injury_event/` S3 prefix, writing to `sport=nfl/` and `sport=football/` respectively. Setting `spark.sql.sources.partitionOverwriteMode = dynamic` scopes each overwrite to only the partitions that job produces — full idempotency, no cross-job collision.

**NFL injury event collapsing.** The `nfl_injuries` source is weekly report observations, not discrete events. The pipeline uses an island-and-gap pattern (PySpark window functions) to reconstruct events: same player + same injury within a 14-day window = one event. Duration is floored at `games_missed * 7` to avoid zero-duration events for single-week islands.

**NULL over inferred values.** NFL source data doesn't provide player nationality. The field is NULL — not defaulted to "USA". NULLs are auditable; fabricated data that's mostly right is silently wrong.

**OIDC over long-lived keys in CI/CD.** GitHub Actions assumes AWS roles via short-lived OIDC tokens, scoped by `sub` claim so PRs get read-only and only main can deploy. No static credentials in GitHub Secrets.

---

## AWS Stack

| Service | Role |
|---|---|
| S3 | Raw and processed data storage; Terraform remote state |
| Glue Data Catalog | Schema registry for both layers |
| Glue ETL (PySpark) | Three transformation jobs |
| Glue Crawlers | Raw-layer schema discovery (one per sport) |
| Athena | Ad-hoc SQL over processed Parquet |
| IAM + OIDC | Glue service role; short-lived CI/CD roles federated to GitHub |

**Intentional scope exclusions:** no S3 lifecycle policies, no MWAA/Airflow orchestration (a cost decision for a portfolio project — Glue Workflows or Step Functions would be the right-sized native fit), and no serving-layer API (Athena/SQL is the natural interface for analytical questions). All acknowledged, all defensible.

---

## AWS CLI Workflow

Everything operational in this project is CLI-only — no console clicks. Infrastructure changes go through Terraform; deploys go through GitHub Actions; job runs, crawlers, and catalog registration are the CLI commands above.

---

## License

MIT
