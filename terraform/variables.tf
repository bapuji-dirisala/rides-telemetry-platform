variable "project" {
  description = "Short project slug used as a prefix for every resource name."
  type        = string
  default     = "rides"
}

variable "environment" {
  description = "Environment short name (local, dev, staging, prod)."
  type        = string
  default     = "local"
  validation {
    condition     = contains(["local", "dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: local, dev, staging, prod."
  }
}

variable "aws_region" {
  description = "AWS region. LocalStack ignores this but still requires one set."
  type        = string
  default     = "us-east-1"
}

variable "use_localstack" {
  description = "Route every AWS endpoint to localhost:4566 (LocalStack Community)."
  type        = bool
  default     = true
}

variable "bucket_name_suffix" {
  description = "Suffix appended to the S3 bucket (empty = short random id)."
  type        = string
  default     = ""
}

variable "firehose_buffer_size_mb" {
  description = "Firehose S3 buffer size in MiB. Must match `FirehoseConfig.buffer_size_mb` in Python."
  type        = number
  default     = 5
}

variable "firehose_buffer_interval_seconds" {
  description = "Firehose S3 buffer flush interval in seconds."
  type        = number
  default     = 60
}

variable "enable_vpc" {
  description = "Phase 8 — provision the VPC + subnets. Off by default (LocalStack VPC is partial)."
  type        = bool
  default     = false
}
