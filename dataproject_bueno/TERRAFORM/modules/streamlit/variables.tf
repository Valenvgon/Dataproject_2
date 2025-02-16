variable "project_id" {
  description = "El ID del proyecto de GCP"
  type        = string
}

variable "region" {
  description = "La región de despliegue"
  type        = string
}

variable "artifact_repo_streamlit" {
  description = "El nombre del repositorio en Artifact Registry"
  type        = string
}

variable "image_name" {
  description = "El nombre de la imagen Docker"
  type        = string
}

variable "affected_topic" {
  description = "El nombre del tópico de afectados"
  type        = string
}

variable "volunteer_topic" {
  description = "El nombre del tópico de voluntarios"
  type        = string
}