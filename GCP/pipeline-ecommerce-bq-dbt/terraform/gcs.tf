# /---------------------------------------------------\
# |     Google Cloud Storage Bucket & Objects         |
# \---------------------------------------------------/


# 1. Create the GCS Bucket
resource "google_storage_bucket" "data_bucket" {
  name          = var.gcs_bucket_name_prefix
  project       = var.project_id
  location      = var.gcs_bucket_location
  storage_class = "STANDARD"

  force_destroy            = true // Set to true only for non-production/testing if you want 'terraform destroy' to delete buckets with objects
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      with_state = "ANY" // This condition means the rule applies regardless of the object's storage class state
      age = 30 // Delete the files after 2 days
    }
  }
}

# 2. Find all CSV files locally within the specified path (including subdirectories)
locals {
  local_csv_files = fileset(var.local_data_path, "**/*.csv")
}

# 3. Upload each found CSV file to the bucket
resource "google_storage_bucket_object" "data_files" {
  for_each = local.local_csv_files

  name   = each.key
  bucket = google_storage_bucket.data_bucket.name

  source = "${var.local_data_path}/${each.key}"

  depends_on = [google_storage_bucket.data_bucket]
}