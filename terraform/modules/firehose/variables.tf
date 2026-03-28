variable "name_prefix" { type = string }
variable "suffix" { type = string }

variable "lakehouse_bucket_arn" {
  description = "ARN of the lakehouse bucket from the s3-lakehouse module."
  type        = string
}

variable "buffer_size_mb" {
  description = "S3 destination buffer size, in MiB."
  type        = number
  default     = 5
}

variable "buffer_interval_seconds" {
  description = "S3 destination buffer flush interval, in seconds."
  type        = number
  default     = 60
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for Firehose delivery errors."
  type        = number
  default     = 14
}

variable "tags" {
  type    = map(string)
  default = {}
}
