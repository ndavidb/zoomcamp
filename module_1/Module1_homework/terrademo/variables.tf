variable "credentials" {
  description = "My Credentials"
  default = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default = "de-course-410902"
}

variable "region" {
  description = "Project region"
  default = "us-central1"
}

variable "location" {
  description = "Project location"
  default = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default = "de-course-410902-terra-bucket"
}


variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default = "STANDARD"
}