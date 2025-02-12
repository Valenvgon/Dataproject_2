variable "project_id" {
  description = "El ID del proyecto en Google Cloud"
  type        = string
}

variable "region" {
  description = "Región donde se desplegarán los recursos"
  type        = string
}

variable "bucket_dataflow" {
  description = "Nombre del bucket para Dataflow"
  type        = string
}

variable "artifact_repo" {
  description = "nombre del repo de artifact"
  type        = string
}

variable "image_name" {
  description = "Nombre de la imagen"
  type        = string
}

variable "cloud_run_service" {
  description = "Nombre del servicio de cloud run"
  type        = string
}

variable "bigquery_dataset" {
  description = "Nombre del biquery dataset"
  type        = string
}