# -----------------------------------------------------------------------------
# Trust policy — who is allowed to assume GlueServiceRole
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

# -----------------------------------------------------------------------------
# Permission policies — what the role can do once assumed
# -----------------------------------------------------------------------------

# Bucket ARN is referenced from the S3 resource rather than hardcoded so that
# renaming the bucket propagates through IAM without a second edit.
data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.sports_injury_pipeline_bucket.arn,
      "${aws_s3_bucket.sports_injury_pipeline_bucket.arn}/*",
    ]
  }
}

data "aws_iam_policy_document" "glue_s3_delete" {
  statement {
    effect  = "Allow"
    actions = ["s3:DeleteObject", "s3:DeleteObjectVersion"]
    resources = [
      "${aws_s3_bucket.sports_injury_pipeline_bucket.arn}/*",
    ]
  }
}

# -----------------------------------------------------------------------------
# The role itself
# -----------------------------------------------------------------------------
resource "aws_iam_role" "glue_service_role" {
  name               = "GlueServiceRole"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

# -----------------------------------------------------------------------------
# Managed policy attachment — standard AWS-authored Glue permissions
# (CloudWatch logs, catalog reads, etc.). Content owned by AWS; we only own
# the attachment.
# -----------------------------------------------------------------------------
resource "aws_iam_role_policy_attachment" "glue_service_role_managed" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# -----------------------------------------------------------------------------
# Inline policies — bucket-scoped S3 permissions
# Names must match what's already in AWS for a clean import.
# -----------------------------------------------------------------------------
resource "aws_iam_role_policy" "glue_s3_access" {
  name   = "GlueS3Access"
  role   = aws_iam_role.glue_service_role.id
  policy = data.aws_iam_policy_document.glue_s3_access.json
}

resource "aws_iam_role_policy" "glue_s3_delete" {
  name   = "GlueS3DeletePolicy"
  role   = aws_iam_role.glue_service_role.id
  policy = data.aws_iam_policy_document.glue_s3_delete.json
}
