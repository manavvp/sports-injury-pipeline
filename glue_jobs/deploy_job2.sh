#!/usr/bin/env bash
# Deploy and run Job 2 (NFL Facts).
#
# Depends on Job 1 (dimensions) having completed.
#
# Usage:
#   ./deploy_job2.sh upload   # upload script to S3 only
#   ./deploy_job2.sh sync     # upload + create-or-update Glue job definition
#   ./deploy_job2.sh run      # start a job run (assumes job already exists)
#   ./deploy_job2.sh all      # upload + sync + run, then tail status
set -euo pipefail

BUCKET="sports-injury-pipeline-manav"
JOB_NAME="job2_nfl_facts"
ROLE_NAME="GlueServiceRole"
SCRIPT_LOCAL="$(dirname "$0")/job2_nfl_facts.py"
SCRIPT_S3="s3://${BUCKET}/scripts/job2_nfl_facts.py"
TMP_LOG_URI="s3://${BUCKET}/glue-logs/"

ROLE_ARN="$(aws iam get-role --role-name "${ROLE_NAME}" --query 'Role.Arn' --output text)"

upload_script() {
  echo "Uploading ${SCRIPT_LOCAL} -> ${SCRIPT_S3}"
  aws s3 cp "${SCRIPT_LOCAL}" "${SCRIPT_S3}"
}

sync_job() {
  if aws glue get-job --job-name "${JOB_NAME}" >/dev/null 2>&1; then
    echo "Updating existing Glue job: ${JOB_NAME}"
    aws glue update-job --job-name "${JOB_NAME}" --job-update "{
      \"Role\": \"${ROLE_ARN}\",
      \"Command\": {
        \"Name\": \"glueetl\",
        \"ScriptLocation\": \"${SCRIPT_S3}\",
        \"PythonVersion\": \"3\"
      },
      \"DefaultArguments\": {
        \"--job-language\": \"python\",
        \"--enable-metrics\": \"true\",
        \"--enable-continuous-cloudwatch-log\": \"true\",
        \"--TempDir\": \"${TMP_LOG_URI}\"
      },
      \"GlueVersion\": \"4.0\",
      \"WorkerType\": \"G.1X\",
      \"NumberOfWorkers\": 2,
      \"Timeout\": 30
    }"
  else
    echo "Creating Glue job: ${JOB_NAME}"
    aws glue create-job \
      --name "${JOB_NAME}" \
      --role "${ROLE_ARN}" \
      --command "Name=glueetl,ScriptLocation=${SCRIPT_S3},PythonVersion=3" \
      --default-arguments "{
        \"--job-language\": \"python\",
        \"--enable-metrics\": \"true\",
        \"--enable-continuous-cloudwatch-log\": \"true\",
        \"--TempDir\": \"${TMP_LOG_URI}\"
      }" \
      --glue-version "4.0" \
      --worker-type "G.1X" \
      --number-of-workers 2 \
      --timeout 30
  fi
}

run_job() {
  RUN_ID="$(aws glue start-job-run --job-name "${JOB_NAME}" --query 'JobRunId' --output text)"
  echo "Started run: ${RUN_ID}"
  echo "Tail with: aws glue get-job-run --job-name ${JOB_NAME} --run-id ${RUN_ID} --query 'JobRun.JobRunState'"
}

case "${1:-all}" in
  upload) upload_script ;;
  sync)   upload_script; sync_job ;;
  run)    run_job ;;
  all)    upload_script; sync_job; run_job ;;
  *)      echo "Unknown subcommand: ${1}"; exit 1 ;;
esac
