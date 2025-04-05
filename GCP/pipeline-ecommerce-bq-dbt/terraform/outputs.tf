output "airflow_service_account_key_base64" {
  description = "The airflow service account key, base64 encoded. Decode this and save to a JSON file."
  value       = google_service_account_key.airflow_service_account_key.private_key
  sensitive   = true # Mark as sensitive to prevent showing in plain text logs/outputs
}