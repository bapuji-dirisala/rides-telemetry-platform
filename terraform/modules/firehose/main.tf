// Kinesis Data Firehose delivery stream — direct put (no Kinesis
// Data Streams in front) → S3 with GZIP compression + Hive-style
// partitioning. The reader side (rides_telemetry.kinesis.reader) uses
// the same `event_type=/year=/month=/day=/hour=/` prefix layout.
//
// Design decisions:
// - `extended_s3_configuration` with `dynamic_partitioning_configuration.enabled = true`
//   so Firehose evaluates record-level jq expressions to build the
//   partition path. One stream can feed every event type.
// - `cloudwatch_logging_options.enabled = true` so delivery errors
//   show up in CloudWatch Logs (feeds the Phase 11 Grafana dashboard).
// - `buffering_interval` and `buffering_size` are first-class vars so
//   the Python producer `FirehoseConfig` can be kept in sync.

locals {
  stream_name = "${var.name_prefix}-rides-events-${var.suffix}"
}

resource "aws_iam_role" "firehose" {
  name = "${local.stream_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = var.tags
}

data "aws_iam_policy_document" "firehose_perms" {
  statement {
    sid    = "S3Delivery"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      var.lakehouse_bucket_arn,
      "${var.lakehouse_bucket_arn}/*",
    ]
  }

  statement {
    sid       = "CloudWatchLogs"
    effect    = "Allow"
    actions   = ["logs:PutLogEvents"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  role   = aws_iam_role.firehose.id
  name   = "firehose-perms"
  policy = data.aws_iam_policy_document.firehose_perms.json
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.stream_name}"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "s3-delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

resource "aws_kinesis_firehose_delivery_stream" "rides" {
  name        = local.stream_name
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose.arn
    bucket_arn         = var.lakehouse_bucket_arn
    buffering_size     = var.buffer_size_mb
    buffering_interval = var.buffer_interval_seconds
    compression_format = "GZIP"

    // Hive-style partition prefix. Keep the jq expression synced with
    // rides_telemetry.kinesis.config.hive_partition_prefix().
    prefix              = "bronze/rides/event_type=!{partitionKeyFromQuery:event_type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "bronze/_errors/rides/!{firehose:error-output-type}/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}/"

    dynamic_partitioning_configuration {
      enabled = true
    }

    processing_configuration {
      enabled = true

      processors {
        type = "MetadataExtraction"

        parameters {
          parameter_name  = "MetadataExtractionQuery"
          parameter_value = "{event_type:.event_type}"
        }

        parameters {
          parameter_name  = "JsonParsingEngine"
          parameter_value = "JQ-1.6"
        }
      }
    }

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }

  tags = var.tags
}
