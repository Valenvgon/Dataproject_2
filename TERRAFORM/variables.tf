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

variable "artifact_repo_streamlit"{
  description = "el nombre del repo en Artifact de Streamlit"
  type = string
}

variable "image_name" {
  description = "El nombre de la imagen Docker a construir"
  type        = string
}

variable "image_name_dflow" {
  description = "El nombre de la imagen Docker a construir"
  type        = string
}

variable "image_name_streamlit"{
  description = "el nombre de la imagen de streamlit"
  type = string
}

variable "bucket_dataflow" {
  description = "El nombre del bucket de GCS para Dataflow (temp y staging)"
  type        = string
}

variable "bq_dataset" {
  description = "El nombre del dataset de BigQuery"
  type        = string
}

variable "bq_dataset_streamlit" {
  description = "el nombre del dataset que es igual que el de arriba para que coja los datos del dataset correspondiente"
  type = string
}

variable "matched_table" {
  description = "El nombre de la tabla de BigQuery para registros matcheados"
  type        = string
  default     = "matched"
}

variable "bq_table_streamlit" {
  description = "el nombre de la tabla de no matcheados para que se vea en streamlit"
  type = string
  default = "unmatcehd"
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

variable "bq_table_streamlit_matched"{
  description = "el nombre de la tabla de los matcheados"
  type= string
}
