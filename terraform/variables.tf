variable "credentials" {
  description = "Path to your Service Account JSON file."
  default     = "/your/path/to/service-account.json"
}

variable "project" {
  description = "Your Google Cloud Project ID."
  default     = "Your Project ID"
}

variable "region" {
  description = "Region for resources."
  default     = "Your preferred region"
}

variable "location" {
  description = "Project Location."
  default     = "Your preferred location"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name."
  default     = "Your Default dataset name"
}

variable "gcs_bucket_name" {
  description = "Storage Bucket Name."
  default     = "Your Storage Bucket Name" # Ensure this is unique across GCS
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class."
  default     = "STANDARD" # Default storage class for GCS bucket
}
