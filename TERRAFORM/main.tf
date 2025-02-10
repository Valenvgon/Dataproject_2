<<<<<<< Updated upstream
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
=======
terraform {
  required_version = ">= 1.3.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "data_generator" {
  source       = "./modules/Data_generator"
  project_id   = var.project_id
  region       = var.region
  artifact_repo = var.artifact_repo
}

module "dataflow" {
  source          = "./modules/Dataflow"
  project_id      = var.project_id
  region          = var.region
  dataflow_bucket = "${var.project_id}-dataflow-bucket"
>>>>>>> Stashed changes
}