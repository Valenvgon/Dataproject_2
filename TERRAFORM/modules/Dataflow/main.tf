resource "google_pubsub_topic" "matched" {
  name    = "matched"
  project = var.project_id
}

resource "google_pubsub_topic" "no_matched" {
  name    = "no_matched"
  project = var.project_id
}

resource "google_storage_bucket" "dataflow_bucket" {
  name          = var.bucket_dataflow
  location      = var.region
  storage_class = "STANDARD"
}

resource "google_bigquery_dataset" "volunteer_matching" {
  dataset_id = "volunteer_matching"
  project    = var.project_id
  location   = var.region
}
variable "project_id" {
  description = "El ID del proyecto"
  type        = string
}

variable "region" {
  description = "La región de despliegue"
  type        = string
}

variable "bucket_dataflow" {
  description = "nombre del bucket de dataflow"
  type        = string
}