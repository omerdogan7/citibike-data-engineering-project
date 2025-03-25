terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}
# Create a GCS bucket
resource "google_storage_bucket" "data_lake_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = var.gcs_storage_class

  # Optional but recommended settings
  force_destroy               = true
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # days
    }
  }
}

# Create BigQuery Dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  project    = var.project
  location   = var.location

  # Optional but recommended settings
  delete_contents_on_destroy = true
}

