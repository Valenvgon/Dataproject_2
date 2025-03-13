############################
# 1. Bucket para Dataflow
############################
resource "google_storage_bucket" "dataflow_bucket" {
  name          = var.bucket_dataflow
  location      = var.region
  storage_class = "STANDARD"
  project       = var.project_id
}

resource "google_storage_bucket_object" "tmp_folder" {
  name    = "tmp/"
  bucket  = google_storage_bucket.dataflow_bucket.name
  content = " "
}

resource "google_storage_bucket_object" "stg_folder" {
  name    = "stg/"
  bucket  = google_storage_bucket.dataflow_bucket.name
  content = " "
}


############################
# 4. Dataset de BigQuery
############################
resource "google_bigquery_dataset" "dataflow_dataset" {
  dataset_id = var.bq_dataset
  project    = var.project_id
  location   = var.region
}

############################
# 5. Artifact Registry Repository
############################
resource "google_artifact_registry_repository" "repo" {
  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_repo_dataflow
  format        = "DOCKER"
}

############################
# 6. Build & Push de la Imagen Docker
############################
resource "null_resource" "build_and_push_docker" {
  depends_on = [google_artifact_registry_repository.repo]

  provisioner "local-exec" {
    working_dir = path.module
    command = <<-EOT
      docker build --platform=linux/amd64 -t europe-west1-docker.pkg.dev/${var.project_id}/${var.artifact_repo_dataflow}/${var.image_name_dflow}:latest .
      docker push europe-west1-docker.pkg.dev/${var.project_id}/${var.artifact_repo_dataflow}/${var.image_name_dflow}:latest
    EOT
  }
}



############################
# 7. Cloud Run V2 Job para el Pipeline (Dataflow)
############################
resource "google_cloud_run_v2_job" "dataflow_job" {
  name     = "dataflow-job"
  location = var.region
  project  = var.project_id

  template {
    template {
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo_dataflow}/${var.image_name_dflow}:latest"
        env {
          name  = "PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "REGION"
          value = var.region
        }
        env {
          name  = "AFFECTED_SUB"
          value = var.affected_sub
        }
        env {
          name  = "VOLUNTEER_SUB"
          value = var.volunteer_sub
        }
        env {
          name  = "AFFECTED_TOPIC"
          value = var.affected_topic
        }
        env {
          name  = "VOLUNTEER_TOPIC"
          value = var.volunteer_topic
        }
        env {
          name  = "BQ_DATASET"
          value = var.bq_dataset
        }
        env {
          name  = "MATCHED_TABLE"
          value = var.matched_table
        }
        env {
          name  = "UNMATCHED_TABLE"
          value = var.unmatched_table
        }
        env {
          name  = "TEMP_LOCATION"
          value = var.temp_location
        }
        env {
          name  = "STAGING_LOCATION"
          value = var.staging_location
        }
        resources {
          limits = {
            memory = "16Gi"
            cpu    = "4"
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
