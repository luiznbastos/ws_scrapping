variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "ws-analytics"
}

variable "job_name" {
  description = "Job name"
  type        = string
  default     = "scrapping"
}

variable "vcpus" {
  description = "Number of vCPUs for the job"
  type        = number
  default     = 2
}

variable "memory" {
  description = "Memory in MB for the job"
  type        = number
  default     = 4096
}

variable "timeout_seconds" {
  description = "Job timeout in seconds"
  type        = number
  default     = 3600
}



