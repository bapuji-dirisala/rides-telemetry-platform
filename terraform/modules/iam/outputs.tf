output "producer_role_arn" {
  value = aws_iam_role.producer.arn
}

output "databricks_role_arn" {
  value = aws_iam_role.databricks.arn
}
