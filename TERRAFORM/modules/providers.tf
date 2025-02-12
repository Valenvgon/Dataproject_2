terraform {
  backend "gcs" {
    bucket = "terraform-bucket-dataproject-2425"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}