provider "aws" {
  region = local.region
}

data "aws_caller_identity" "this" {}

locals {
  region = "us-west-2"
  tags   = {}
  env    = "non-prod"
}