variable "project_id" {
  description = "El ID del proyecto de GCP"
  type        = string
}

variable "region" {
  description = "La región (ej. europe-west1)"
  type        = string
}

variable "bucket_dataflow" {
  description = "El nombre del bucket GCS para Dataflow"
  type        = string
}

variable "bq_dataset" {
  description = "El nombre del dataset de BigQuery"
  type        = string
}

variable "artifact_repo_dataflow" {
  description = "El nombre del repositorio en Artifact Registry para Dataflow"
  type        = string
}

variable "image_name_dflow" {
  description = "El nombre de la imagen Docker a construir para Dataflow"
  type        = string
}

variable "affected_sub" {
  description = "El nombre de la subscripción de Pub/Sub para afectados"
  type        = string
}

variable "volunteer_sub" {
  description = "El nombre de la subscripción de Pub/Sub para voluntarios"
  type        = string
}

variable "affected_topic" {
  description = "El nombre del tópico de Pub/Sub para afectados"
  type        = string
}

variable "volunteer_topic" {
  description = "El nombre del tópico de Pub/Sub para voluntarios"
  type        = string
}

variable "matched_table" {
  description = "La tabla de BigQuery para registros matcheados"
  type        = string
  default     = "matched_table"
}

variable "unmatched_table" {
  description = "La tabla de BigQuery para registros no matcheados"
  type        = string
  default     = "non_matched_table"
}

variable "temp_location" {
  description = "Ruta GCS para archivos temporales de Dataflow (ej. gs://bucket/tmp)"
  type        = string
}

variable "staging_location" {
  description = "Ruta GCS para archivos de staging de Dataflow (ej. gs://bucket/stg)"
  type        = string
}
