variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "artifact_repo_streamlit" {
  type = string
}

variable "image_name_streamlit" {
  type = string
}

variable "affected_topic" {
  type = string
}

variable "volunteer_topic" {
  type = string
}

variable "bq_table_streamlit" {
  description = "el nombre de la tabla de no matcheados para que se vea en streamlit"
  type = string
  default = "unmatcehd"
}

variable "bq_dataset_streamlit" {
  description = "el nombre del dataset que es igual que el de arriba para que coja los datos del dataset correspondiente"
  type = string
}