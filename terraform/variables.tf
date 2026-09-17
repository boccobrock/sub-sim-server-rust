variable "aws_region" {
  description = "AWS region containing the VPC and ECR repository."
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for the Lambda security group."
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs in the VPC where Lambda ENIs are created."
  type        = list(string)
}

variable "ecr_repository" {
  description = "Name of the existing ECR repository containing the image."
  type        = string
}

variable "image_tag" {
  description = "Immutable image tag to deploy."
  type        = string
}

variable "lambda_name" {
  description = "Lambda function name."
  type        = string
  default     = "sub-sim-server-rust"
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 30
}

variable "lambda_memory" {
  description = "Lambda memory allocation in MB."
  type        = number
  default     = 512
}
