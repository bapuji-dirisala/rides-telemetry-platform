// Project root composition — Phases 7 + 8.
//
// Phase 7: s3-lakehouse + firehose
// Phase 8: iam + vpc

resource "random_id" "suffix" {
  byte_length = 3
}

locals {
  suffix      = var.bucket_name_suffix != "" ? var.bucket_name_suffix : random_id.suffix.hex
  name_prefix = "${var.project}-${var.environment}"

  common_tags = {
    Project     = "rides-telemetry-platform"
    Environment = var.environment
  }
}

module "s3_lakehouse" {
  source = "./modules/s3-lakehouse"

  name_prefix = local.name_prefix
  suffix      = local.suffix
  tags        = local.common_tags
}

module "firehose" {
  source = "./modules/firehose"

  name_prefix             = local.name_prefix
  suffix                  = local.suffix
  lakehouse_bucket_arn    = module.s3_lakehouse.bucket_arn
  buffer_size_mb          = var.firehose_buffer_size_mb
  buffer_interval_seconds = var.firehose_buffer_interval_seconds
  tags                    = local.common_tags
}

module "iam" {
  source = "./modules/iam"

  name_prefix                  = local.name_prefix
  suffix                       = local.suffix
  lakehouse_bucket_arn         = module.s3_lakehouse.bucket_arn
  firehose_delivery_stream_arn = module.firehose.delivery_stream_arn
  tags                         = local.common_tags
}

module "vpc" {
  count  = var.enable_vpc ? 1 : 0
  source = "./modules/vpc"

  name_prefix = local.name_prefix
  suffix      = local.suffix
  tags        = local.common_tags
}
