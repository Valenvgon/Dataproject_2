resource "google_artifact_registry_repository" "repo_streamlit" {
  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_repo_streamlit
  format        = "DOCKER"
}

resource "null_resource" "build_and_push_docker_streamlit" {
  depends_on = [google_artifact_registry_repository.repo_streamlit]

  provisioner "local-exec" {
    working_dir = path.module
    command = <<-EOT
      docker build --platform=linux/amd64 -t ${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo_streamlit}/${var.image_name_streamlit}:latest .
      docker push ${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo_streamlit}/${var.image_name_streamlit}:latest
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
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo_streamlit}/${var.image_name_streamlit}:latest"

        ports {
          container_port = 8501
        }

        env {
          name  = "PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "AFFECTED_TOPIC"
          value = var.affected_topic
        }
        env {
          name  = "VOLUNTEER_TOPIC"
          value = var.volunteer_topic
        }
        env{
          name = "BQ_DATASET"
          value= var.bq_dataset_streamlit
        }
        env{
          name = "BQ_TABLE"
          value = var.bq_table_streamlit
        }
        resources {
          limits = {
            memory = "16Gi"
            cpu    = "4"
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
    google_artifact_registry_repository.repo_streamlit,
    null_resource.build_and_push_docker_streamlit
  ]
}

resource "google_cloud_run_service_iam_policy" "allow_public_access_streamlit" {
  location = google_cloud_run_service.streamlit_service.location
  project  = google_cloud_run_service.streamlit_service.project
  service  = google_cloud_run_service.streamlit_service.name

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
