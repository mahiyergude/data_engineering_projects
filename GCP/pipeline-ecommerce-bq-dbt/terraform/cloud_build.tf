
variable "branch_name" {
  description = "The branch name to trigger builds on (e.g., 'main', 'master')."
  type        = string
  default     = "main"
}

variable "repo_owner" {
  description = "The owner (username or organization) of the GitHub repository."
  type        = string
  default     = "mahiyergude"
}

variable "repo_name" {
  description = "The name of the GitHub repository."
  type        = string
  default     = "data_engineering_projects_temp"
}

variable "cloudbuild_connection_name" {
  description = "A unique name for the Cloud Build Connection resource."
  type        = string
  default     = "github-connection"
}

# Cloud Build Connection to GitHub
# 1. Cloud Build Trigger
resource "google_cloudbuild_trigger" "main_branch_trigger" {
  project  = var.project_id
  location = "global"
  name     = "pipeline-ecommerce-bq-dbt-trigger"
  description = "Triggers build on push to ${var.branch_name} branch"
  filename  = "GCP/pipeline-ecommerce-bq-dbt/cloud_run_dbt/cloudbuild.yml"

  github {
    owner = var.repo_owner
    name  = var.repo_name
    push {
      # Use regex for exact branch match. Use ".*" for any branch.
      branch = "^${var.branch_name}$"
    }
  }

  included_files = ["GCP/pipeline-ecommerce-bq-dbt/cloud_run_dbt/**"]
  service_account = "projects/ecommerce-demo-123/serviceAccounts/${google_service_account.cloud_build_service_account.email}"
  include_build_logs = "INCLUDE_BUILD_LOGS_WITH_STATUS"

}



