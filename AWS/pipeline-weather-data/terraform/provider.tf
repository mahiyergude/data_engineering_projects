provider "aws" {
  region  = "eu-central-1" # Change this to your desired region
#   profile = "dev"
}

data "aws_caller_identity" "current" {}

