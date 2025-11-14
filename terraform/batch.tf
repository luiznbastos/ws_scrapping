# AWS Batch Job Definition for Scrapping
resource "aws_batch_job_definition" "this" {
  name                  = "${var.project_name}-${var.job_name}"
  type                  = "container"
  platform_capabilities = ["EC2"]

  container_properties = jsonencode({
    image      = "${data.aws_ssm_parameter.ecr_url.value}:latest"
    vcpus      = var.vcpus
    memory     = var.memory
    jobRoleArn = aws_iam_role.job_role.arn

    environment = [
      {
        name  = "S3_BUCKET"
        value = data.aws_ssm_parameter.analytics_bucket.value
      },
      {
        name  = "S3_PREFIX"
        value = "raw"
      },
      {
        name  = "JOB_NAME"
        value = var.job_name
      },
      {
        name  = "AWS_REGION"
        value = var.aws_region
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/aws/batch/${var.project_name}"
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = var.job_name
      }
    }
  })

  retry_strategy {
    attempts = 3
    evaluate_on_exit {
      action       = "RETRY"
      on_exit_code = "1"
    }
  }

  timeout {
    attempt_duration_seconds = var.timeout_seconds
  }

  tags = {
    Name    = "${var.project_name}-${var.job_name}-job"
    Project = var.project_name
    Job     = var.job_name
  }
}

# Store job ARN for orchestrator to reference
resource "aws_ssm_parameter" "job_arn" {
  name  = "/${var.project_name}/batch/jobs/${var.job_name}/arn"
  type  = "String"
  value = aws_batch_job_definition.this.arn

  tags = {
    Name    = "${var.project_name}-${var.job_name}-job-arn-param"
    Project = var.project_name
  }
}



