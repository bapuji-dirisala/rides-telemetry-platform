// Dual-mode provider: `use_localstack = true` routes every AWS SDK
// call to http://localhost:4566 so `terraform plan` + `terraform apply`
// run against the LocalStack Community image, keeping the project
// $0-forever by default. Flip to false (or omit) to hit real AWS.

provider "aws" {
  region = var.aws_region

  skip_credentials_validation = var.use_localstack
  skip_metadata_api_check     = var.use_localstack
  skip_requesting_account_id  = var.use_localstack

  s3_use_path_style = var.use_localstack

  default_tags {
    tags = {
      Project     = "rides-telemetry-platform"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }

  dynamic "endpoints" {
    for_each = var.use_localstack ? [1] : []
    content {
      firehose = "http://localhost:4566"
      iam      = "http://localhost:4566"
      kinesis  = "http://localhost:4566"
      s3       = "http://localhost:4566"
      sts      = "http://localhost:4566"
    }
  }
}
