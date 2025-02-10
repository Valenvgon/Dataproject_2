resource "google_pubsub_topic" "volunteer" {
  name = "volunteer"
}

resource "google_pubsub_topic" "affected" {
  name = "affected"
}

resource "google_artifact_registry_repository" "repo" {
  provider      = google
  location      = "europe-west1"
  repository_id = "data-generator-repo"
  format        = "DOCKER"
}

resource "google_cloud_run_service" "generator_service" {
  name     = "generator-service"
  location = var.region

  template {
    spec {
      containers {
        image = "europe-west1-docker.pkg.dev/${var.project_id}/${var.artifact_repo}/generator:latest"
      }
    }
  }
}

resource "google_cloudbuild_trigger" "build_generator" {
  name        = "build-generator"
  description = "Trigger para construir y subir la imagen de generator.py"

  github {
    owner = "tu-usuario"
    name  = "tu-repo"
    push {
      branch = "main"
    }
  }

  build {
    step {
      name       = "gcr.io/cloud-builders/docker"
      args       = ["build", "-t", "europe-west1-docker.pkg.dev/${var.project_id}/${var.artifact_repo}/generator:latest", "."]
      dir        = "terraform/modules/Data_Generator"
    }

    step {
      name = "gcr.io/cloud-builders/docker"
      args = ["push", "europe-west1-docker.pkg.dev/${var.project_id}/${var.artifact_repo}/generator:latest"]
    }
  }
}