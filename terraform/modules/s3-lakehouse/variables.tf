variable "name_prefix" { type = string }
variable "suffix" { type = string }

variable "kms_key_id" {
  description = "Optional CMK ARN. Empty = SSE-S3 (AES256)."
  type        = string
  default     = ""
}

variable "noncurrent_tier_days" {
  description = "Days before non-current versions tier to STANDARD_IA."
  type        = number
  default     = 30
}

variable "noncurrent_expire_days" {
  description = "Days before non-current versions are deleted."
  type        = number
  default     = 365
}

variable "delta_log_retain_days" {
  description = "Days to keep non-current Delta transaction log files."
  type        = number
  default     = 180
}

variable "tags" {
  type    = map(string)
  default = {}
}
