# Reference central infrastructure via SSM parameters
data "aws_ssm_parameter" "ecr_url" {
  name = "/${var.project_name}/ecr/${var.job_name}/url"
}

data "aws_ssm_parameter" "job_queue_arn" {
  name = "/${var.project_name}/batch/job-queue-arn"
}

data "aws_ssm_parameter" "analytics_bucket" {
  name = "/${var.project_name}/s3/analytics/name"
}

# Fetch shared policies from central repo
data "aws_iam_policy" "ssm_policy" {
  name = "${var.project_name}-ssm-policy"
}

data "aws_iam_policy" "cloudwatch_policy" {
  name = "${var.project_name}-cloudwatch-policy"
}



