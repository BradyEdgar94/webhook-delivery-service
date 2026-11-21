variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Name prefix for every resource"
  type        = string
  default     = "hooklane"
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "node_instance_type" {
  description = "EKS worker node type. 4 vCPU / 16 GB each."
  type        = string
  default     = "m6i.xlarge"
}

variable "node_count" {
  description = "Fixed node count for the load test (KEDA scales pods, not nodes)."
  type        = number
  default     = 3
}

variable "db_instance_class" {
  type    = string
  default = "db.m6g.large"
}

variable "db_password" {
  description = "RDS master password. Pass with TF_VAR_db_password; never commit it."
  type        = string
  sensitive   = true
}
