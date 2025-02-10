variable "project_id" {
  description = "El ID del proyecto de Google Cloud"
  type        = string
  default     = "data-project-2425"
}

variable "region" {
  description = "Región de despliegue en Google Cloud"
  type        = string
  default     = "europe-west1"
}

variable "artifact_repo" {
  description = "Nombre del repositorio de Artifact Registry"
  type        = string
  default     = "volunteer-registry"
}

variable "dataflow_bucket" {
  description = "Bucket de Cloud Storage para Dataflow"
  type        = string
  default     = "dataflow_bucket_dataproject_2425"
}