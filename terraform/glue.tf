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
  }

  execution_property {
    max_concurrent_runs = 1
  }
}
