variable "name_prefix" { type = string }
variable "suffix" { type = string }

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "tags" {
  type    = map(string)
  default = {}
}
