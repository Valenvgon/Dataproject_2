provider "google" {
    project = var.project_id
    region  = "europe-west1"
}

resource "google_pubsub_topic" "topic1" {
    name = "volunteers"
    project = var.project_id
}

resource "google_pubsub_topic" "topic2" {
    name = "affected"
    project = var.project_id
}

resource "google_pubsub_topic" "topic3" {
    name = "match_data"
    project = var.project_id
}

resource "google_storage_bucket" "dataflow_bucket" {
    name     = "dataflow-bucket-2425"
    location = "EU"
}

resource "google_bigquery_dataset" "dataset" {
    dataset_id = "dataflow_dataset"
    location   = "EU"
}