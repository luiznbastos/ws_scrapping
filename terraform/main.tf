terraform {
  backend "s3" {
    bucket         = "terraform-ws-analytics-infra"
    key            = "state/ws_scrapping/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-locks"
  }
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.58"
    }
  }
}

provider "aws" {
  region = var.aws_region
}


