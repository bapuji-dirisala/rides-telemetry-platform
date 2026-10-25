// Single S3 bucket for the Delta lakehouse. The bronze/silver/gold
// layers share one bucket to minimise cross-bucket IAM; each layer
// is a top-level prefix.
//
// Design decisions:
// - Versioning ON so Delta's _delta_log/ history can survive bronze
//   compactions.
// - Public access blocked (hard default).
// - Lifecycle rule tiers non-current versions to IA after 30 days;
//   the _delta_log/ prefix is left untouched.
// - Server-side encryption with AWS-managed KMS. Flip to CMK by
//   setting kms_key_id.

resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.name_prefix}-lakehouse-${var.suffix}"

  tags = var.tags
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_id == "" ? "AES256" : "aws:kms"
      kms_master_key_id = var.kms_key_id == "" ? null : var.kms_key_id
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "tier-noncurrent-versions"
    status = "Enabled"

    filter {
      prefix = ""
    }

    noncurrent_version_transition {
      noncurrent_days = var.noncurrent_tier_days
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_expire_days
    }
  }

  rule {
    id     = "keep-delta-log-hot"
    status = "Enabled"

    filter {
      prefix = "_delta_log/"
    }

    noncurrent_version_expiration {
      noncurrent_days = var.delta_log_retain_days
    }
  }
}
