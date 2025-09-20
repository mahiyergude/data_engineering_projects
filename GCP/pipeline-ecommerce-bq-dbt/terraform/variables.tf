##### PROJECT VARIABLES #####

variable "project_id" {
  description = "The Google Cloud project ID where resources will be created."
  type        = string
  default     = "ecommerce-demo-123"
}

variable "region" {
  description = "The GCP region for Cloud Build resources."
  type        = string
  default     = "us-central1" 
}

##### IAM VARIABLES ######

variable "dbt_service_account_id" {
  description = "The desired ID for the DBT service account (e.g., 'dbt-runner-sa')."
  type        = string
  default     = "dbt-runner-sa"
}

variable "airflow_service_account_id" {
  description = "The desired ID for the airflow service account (e.g., 'airflow-runner-sa')."
  type        = string
  default     = "airflow-runner-sa"
}

variable "cloud_build_service_account_id" {
  description = "The desired ID for the cloudbuild service account (e.g., 'cloudbuild-runner-sa')."
  type        = string
  default     = "cloudbuild-runner-sa"
}

variable "dbt_service_account_display_name" {
  description = "The display name for the dbt service account."
  type        = string
  default     = "dbt Runner Service Account"
}

variable "airflow_service_account_display_name" {
  description = "The display name for the airflow service account."
  type        = string
  default     = "airflow Runner Service Account"
}

variable "cloud_build_service_account_display_name" {
  description = "The display name for the cloud build service account."
  type        = string
  default     = "cloud build Runner Service Account"
}

variable "dbt_required_roles" {
  description = "A list of IAM roles required for the DBT service account (applied at the project level by default)."
  type        = list(string)
  default = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",    
    "roles/bigquery.user",       
  ]
}

variable "airflow_required_roles" {
  description = "A list of IAM roles required for the Airflow service account (applied at the project level by default)."
  type        = list(string)
  default = [
    "roles/bigquery.admin",      
    "roles/storage.admin",
    "roles/run.invoker"
  ]
}


variable "cloud_build_required_roles" {
  description = "A list of IAM roles required for the cloud_build service account (applied at the project level by default)."
  type        = list(string)
  default = [
    "roles/bigquery.admin",
    "roles/storage.admin",
    "roles/artifactregistry.admin",
    "roles/run.developer",
    "roles/logging.logWriter",
    "roles/iam.serviceAccountUser"
  ]
}

##### GCS VARIABLES ######

variable "gcs_bucket_location" {
  description = "The location (region) for the GCS bucket."
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name_prefix" {
  description = "Prefix for the GCS bucket name. Project ID will be appended for uniqueness."
  type        = string
  default     = "ecommerce_data_staging"
}

variable "local_data_path" {
  description = "The local path containing the dataset CSV files to upload."
  type        = string
  default     = "../datasets/olist_ecommerce_dataset"
}
