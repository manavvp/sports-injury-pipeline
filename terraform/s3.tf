resource "aws_s3_bucket" "sports_injury_pipeline_bucket" {
  bucket = "sports-injury-pipeline-manav"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bucket_config" {
  bucket = aws_s3_bucket.sports_injury_pipeline_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = false
  }
}

resource "aws_s3_bucket_public_access_block" "bucket_access_block" {
  bucket                  = aws_s3_bucket.sports_injury_pipeline_bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "bucket_ownership_controls" {
  bucket = aws_s3_bucket.sports_injury_pipeline_bucket.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}