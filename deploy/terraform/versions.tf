# Optional cloud version of Hooklane, for a one-off load test.
# NEVER applied by the build; validate only: terraform init -backend=false && terraform validate
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.90"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = var.name
      Purpose   = "load-test"
      ManagedBy = "terraform"
    }
  }
}
