resource "google_artifact_registry_repository" "repo" {
  provider      = google
  project       = var.project_id
  location      = var.region
  repository_id = "volunteer-matching-repo"
  format        = "DOCKER"
}

resource "google_pubsub_topic" "volunteers" {
  name    = "volunteers"
  project = var.project_id
}

resource "google_pubsub_topic" "affected" {
  name    = "affected"
  project = var.project_id
}

resource "google_cloud_run_service" "generator" {
  name     = "volunteer-generator"
  location = var.region
  project  = var.project_id

  template {
    spec {
      containers {
        image = "europe-west1-docker.pkg.dev/${var.project_id}/volunteer-matching-repo/generator:latest"
        env {
          name  = "PROJECT_ID"
          value = var.project_id
        }
      }
    }
  }
  depends_on = [google_artifact_registry_repository.repo]
}

variable "project_id" {
  description = "El ID del proyecto"
  type        = string
}

variable "region" {
  description = "La región de despliegue"
  type        = string
}