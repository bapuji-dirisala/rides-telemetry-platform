output "delivery_stream_name" {
  value = aws_kinesis_firehose_delivery_stream.rides.name
}

output "delivery_stream_arn" {
  value = aws_kinesis_firehose_delivery_stream.rides.arn
}

output "role_arn" {
  value = aws_iam_role.firehose.arn
}
