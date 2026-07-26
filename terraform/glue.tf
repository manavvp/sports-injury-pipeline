# -----------------------------------------------------------------------------
# Glue ETL jobs — three-job pipeline (dimensions, NFL facts, football facts).
#
# All three jobs share identical config except name. Script location follows a
# naming pattern derived from the job name, so `for_each` over a set of strings
# is the cleanest fit — no fake variation.
#
# If future jobs need different sizing or arguments, switch to `for_each` over
# a map with per-job overrides.
# -----------------------------------------------------------------------------

resource "aws_glue_job" "jobs" {
  for_each = toset([
    "job1_dimensions",
    "job2_nfl_facts",
    "job3_football_facts",
  ])

  name              = each.value
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.sports_injury_pipeline_bucket.bucket}/scripts/${each.value}.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-metrics"                   = "true"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.sports_injury_pipeline_bucket.bucket}/glue-logs/"
    "--enable-continuous-cloudwatch-log" = "true"
    # Makes the transforms/ package importable at runtime.
    # CD packages glue_jobs/transforms/ into this zip on every deploy.
    "--extra-py-files" = "s3://${aws_s3_bucket.sports_injury_pipeline_bucket.bucket}/scripts/transforms.zip"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}

# -----------------------------------------------------------------------------
# Raw-layer crawlers — one per sport. Discover schemas from landed CSVs.
#
# Each crawler scans four S3 prefixes under raw/<sport>/ and registers tables
# in sports_injury_raw prefixed with `<sport>_`. Structure is identical across
# sports, so per-sport config is just the list of source folder names — paths
# are built from bucket + sport + source.
# -----------------------------------------------------------------------------

locals {
  raw_crawlers = {
    nfl = {
      sources = ["injuries", "player_stats", "rosters", "combine"]
    }
    football = {
      sources = ["player_injuries", "player_performances", "player_profiles", "player_market_value"]
    }
  }
}

resource "aws_glue_crawler" "raw" {
  for_each = local.raw_crawlers

  name          = "${each.key}-raw-crawler"
  role          = aws_iam_role.glue_service_role.name
  database_name = aws_glue_catalog_database.glue_raw.name
  table_prefix  = "${each.key}_"

  # One nested s3_target block per source folder. `dynamic` generates them
  # from the per-crawler sources list rather than repeating the block.
  dynamic "s3_target" {
    for_each = each.value.sources
    content {
      path = "s3://${aws_s3_bucket.sports_injury_pipeline_bucket.bucket}/raw/${each.key}/${s3_target.value}/"
    }
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  lineage_configuration {
    crawler_lineage_settings = "DISABLE"
  }
}
