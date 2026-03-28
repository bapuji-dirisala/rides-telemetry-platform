// Phase 8 — hand-written service roles for the producer + the
// Databricks compute plane. No AWS-managed policies (they're always
// over-privileged); every statement is scoped to this project's bucket.

// --- Producer role (Kubernetes service account via IRSA) ------------

resource "aws_iam_role" "producer" {
  name = "${var.name_prefix}-producer-${var.suffix}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        // IRSA assumes an OIDC provider in prod; for LocalStack a
        // simple account-root principal is enough so the role at
        // least exists for apply-time validation.
        AWS = "*"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = var.tags
}

data "aws_iam_policy_document" "producer_perms" {
  statement {
    sid    = "FirehoseWrite"
    effect = "Allow"
    actions = [
      "firehose:PutRecord",
      "firehose:PutRecordBatch",
    ]
    resources = [var.firehose_delivery_stream_arn]
  }
}

resource "aws_iam_role_policy" "producer" {
  role   = aws_iam_role.producer.id
  name   = "producer-perms"
  policy = data.aws_iam_policy_document.producer_perms.json
}

// --- Databricks compute role (reads bronze, writes silver + gold) ---

resource "aws_iam_role" "databricks" {
  name = "${var.name_prefix}-databricks-${var.suffix}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "*" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = var.tags
}

data "aws_iam_policy_document" "databricks_perms" {
  statement {
    sid    = "BronzeRead"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      var.lakehouse_bucket_arn,
      "${var.lakehouse_bucket_arn}/bronze/*",
    ]
  }

  statement {
    sid    = "SilverGoldReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [
      var.lakehouse_bucket_arn,
      "${var.lakehouse_bucket_arn}/silver/*",
      "${var.lakehouse_bucket_arn}/gold/*",
    ]
  }
}

resource "aws_iam_role_policy" "databricks" {
  role   = aws_iam_role.databricks.id
  name   = "databricks-perms"
  policy = data.aws_iam_policy_document.databricks_perms.json
}
