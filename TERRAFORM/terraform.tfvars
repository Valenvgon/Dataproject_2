# ID del proyecto en Google Cloud
project_id = "data-project-2425"

# Región donde se desplegarán los recursos
region = "europe-west1"

# Nombre del bucket para Dataflow
bucket_dataflow = "dataflow_bucket_dataproject_2425"

# Nombre del Artifact Registry
artifact_repo = "volunteers-artifact-repo"

# Nombre de la imagen Docker (sin incluir la URL completa)
image_name = "volunteers-generator"

# Nombre del servicio de Cloud Run
cloud_run_service = "volunteers-generator-service"

# Tópicos de Pub/Sub
pubsub_topics = {
  volunteers  = "volunteers"
  affected    = "affected"
  matched     = "matched"
  no_matched  = "no_matched"
}

# Nombre del dataset de BigQuery
bigquery_dataset = "volunteers_dataset"