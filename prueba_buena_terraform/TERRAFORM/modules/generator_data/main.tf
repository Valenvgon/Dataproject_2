resource "google_artifact_registry_repository" "repo" {
  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_repo
  format        = "DOCKER"
}

resource "google_pubsub_topic" "affected_topic" {
  name    = "affected"
  project = var.project_id
}

resource "google_pubsub_topic" "volunteer_topic" {
  name    = "volunteer"
  project = var.project_id
}

# Creamos una suscripción para el tópico "affected".
resource "google_pubsub_subscription" "affected_sub" {
  name                = "affected-sub"
  topic               = google_pubsub_topic.affected_topic.name
  project             = var.project_id
  ack_deadline_seconds = 60
}

# Creamos una suscripción para el tópico "volunteer".
resource "google_pubsub_subscription" "volunteer_sub" {
  name                = "volunteer-sub"
  topic               = google_pubsub_topic.volunteer_topic.name
  project             = var.project_id
  ack_deadline_seconds = 60
}

resource "null_resource" "build_and_push_docker" {
  depends_on = [google_artifact_registry_repository.repo]

  provisioner "local-exec" {
    working_dir = path.module
    command = <<-EOT
      docker build --platform=linux/amd64 -t europe-west1-docker.pkg.dev/${var.project_id}/${var.artifact_repo}/${var.image_name}:latest .
      docker push europe-west1-docker.pkg.dev/${var.project_id}/${var.artifact_repo}/${var.image_name}:latest
    EOT
  }
}

resource "google_cloud_run_v2_job" "generator_job" {
  name     = "generator-job"
  location = var.region
  project  = var.project_id

  template {
    template {
      containers {
        image = "europe-west1-docker.pkg.dev/${var.project_id}/${var.artifact_repo}/${var.image_name}:latest"
        
        env {
          name  = "PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "AFFECTED_TOPIC"
          value = google_pubsub_topic.affected_topic.name
        }
        env {
          name  = "VOLUNTEER_TOPIC"
          value = google_pubsub_topic.volunteer_topic.name
        }
        resources {
          limits = {
            memory = "2048Mi"
          }
        }
      }
      max_retries = 3
    }
  }

  depends_on = [
    google_artifact_registry_repository.repo,
    null_resource.build_and_push_docker
  ]
}
