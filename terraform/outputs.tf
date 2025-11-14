output "job_definition_arn" {
  description = "ARN of the scrapping job definition"
  value       = aws_batch_job_definition.this.arn
}

output "job_definition_name" {
  description = "Name of the scrapping job definition"
  value       = aws_batch_job_definition.this.name
}

output "job_role_arn" {
  description = "ARN of the job IAM role"
  value       = aws_iam_role.job_role.arn
}