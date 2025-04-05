terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.50.0" // Use a recent version
    }
  }
}

provider "google" {
  project = var.project_id
}