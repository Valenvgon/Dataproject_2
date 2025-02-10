variable "project_id" {
  description = "ID del proyecto de Google Cloud"
  type        = string
}

variable "region" {
  description = "Región de despliegue en Google Cloud"
  type        = string
  default     = "europe-west1"
}

variable "dataflow_bucket" {
  description = "Nombre del bucket para Dataflow"
  type        = string
  default     = "bucket_dataflow"
}