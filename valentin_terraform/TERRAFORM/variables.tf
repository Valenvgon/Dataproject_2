variable "project_id" {
  description = "El ID del proyecto de GCP"
  type        = string
}

variable "region" {
  description = "La región donde se desplegarán los recursos (ej. europe-west1)"
  type        = string
}

variable "artifact_repo_generator" {
  description = "El nombre del repositorio en Artifact Registry"
  type        = string
}

variable "artifact_repo_dataflow" {
  description = "El nombre del repositorio en Artifact Registry"
  type        = string
}

variable "image_name" {
  description = "El nombre de la imagen Docker a construir"
  type        = string
}

variable "image_name_dflow" {
  description = "El nombre de la imagen Docker a construir"
  type        = string
}

variable "bucket_dataflow" {
  description = "El nombre del bucket de GCS para Dataflow (temp y staging)"
  type        = string
}

variable "bq_dataset" {
  description = "El nombre del dataset de BigQuery"
  type        = string
}

variable "matched_table" {
  description = "El nombre de la tabla de BigQuery para registros matcheados"
  type        = string
  default     = "matched"
}

variable "unmatched_table" {
  description = "El nombre de la tabla de BigQuery para registros no matcheados"
  type        = string
  default     = "unmatched"
}

variable "affected_topic" {
  description = "El nombre del tópico de Pub/Sub para afectados"
  type        = string
}

variable "volunteer_topic" {
  description = "El nombre del tópico de Pub/Sub para voluntarios"
  type        = string
}

variable "affected_sub" {
  description = "El nombre del tópico de Pub/Sub para voluntarios"
  type        = string
}

variable "volunteer_sub" {
  description = "El nombre del tópico de Pub/Sub para voluntarios"
  type        = string
}

variable "temp_location" {
  description = "La ruta GCS para archivos temporales de Dataflow (ej. gs://mi-bucket/tmp)"
  type        = string
}

variable "staging_location" {
  description = "La ruta GCS para archivos de staging de Dataflow (ej. gs://mi-bucket/stg)"
  type        = string
}
