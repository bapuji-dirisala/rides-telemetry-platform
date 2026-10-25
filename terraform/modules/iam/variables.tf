variable "name_prefix" { type = string }
variable "suffix" { type = string }

variable "lakehouse_bucket_arn" {
  description = "S3 lakehouse bucket ARN."
  type        = string
}

variable "firehose_delivery_stream_arn" {
  description = "Firehose stream ARN the producer may write to."
  type        = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
