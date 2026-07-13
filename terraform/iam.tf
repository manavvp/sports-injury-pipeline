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


resource "aws_iam_role" "glue_service_role" {
  name               = "GlueServiceRole"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_service_role_managed" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

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
