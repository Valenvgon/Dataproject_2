resource "google_artifact_registry_repository" "repo" {
  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_repo
  format        = "DOCKER"
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

resource "google_cloud_run_service" "streamlit_service" {
  name     = "streamlit-service"
  location = var.region
  project  = var.project_id

  template {
    spec {
      containers {
        image = "europe-west1-docker.pkg.dev/${var.project_id}/${var.artifact_repo}/${var.image_name}:latest"

        ports {
          container_port = 8501
        }

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
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [
    google_artifact_registry_repository.repo,
    null_resource.build_and_push_docker
  ]
}

resource "google_cloud_run_service_iam_policy" "allow_public_access" {
  location    = google_cloud_run_service.streamlit_service.location
  project     = google_cloud_run_service.streamlit_service.project
  service     = google_cloud_run_service.streamlit_service.name

  policy_data = <<EOT
{
  "bindings": [
    {
      "role": "roles/run.invoker",
      "members": ["allUsers"]
    }
  ]
}
EOT

  depends_on = [google_cloud_run_service.streamlit_service]
}