# 1. Create the Service Account
resource "google_service_account" "dbt_service_account" {
  project      = var.project_id
  account_id   = var.dbt_service_account_id
  display_name = var.dbt_service_account_display_name
  description  = "Service Account for DBT to interact with Google Cloud resources."
}

resource "google_service_account" "airflow_service_account" {
  project      = var.project_id
  account_id   = var.airflow_service_account_id
  display_name = var.airflow_service_account_display_name
  description  = "Service Account for Airflow to interact with Google Cloud resources."
}

resource "google_service_account" "cloud_build_service_account" {
  project      = var.project_id
  account_id   = var.cloud_build_service_account_id
  display_name = var.cloud_build_service_account_display_name
  description  = "Service Account for cloud_build to interact with Google Cloud resources."
}


# 2. Create the JSON Key for the Service Account

resource "google_service_account_key" "airflow_service_account_key" {
  service_account_id = google_service_account.airflow_service_account.name 
}

# 3. Grant necessary IAM roles to the Service Account (Project Level Example)
#    Adjust roles and resource level (project, dataset, bucket) as needed in variables.tf or terraform.tfvars.
resource "google_project_iam_member" "dbt_iam_bindings" {
  count   = length(var.dbt_required_roles)
  project = var.project_id
  role    = var.dbt_required_roles[count.index]
  member  = "serviceAccount:${google_service_account.dbt_service_account.email}"
}

resource "google_project_iam_member" "airflow_iam_bindings" {
  count   = length(var.airflow_required_roles)
  project = var.project_id
  role    = var.airflow_required_roles[count.index]
  member  = "serviceAccount:${google_service_account.airflow_service_account.email}"
}

resource "google_project_iam_member" "cloud_build_iam_bindings" {
  count   = length(var.cloud_build_required_roles)
  project = var.project_id
  role    = var.cloud_build_required_roles[count.index]
  member  = "serviceAccount:${google_service_account.cloud_build_service_account.email}"
}
