module "data_generator" {
  source         = "./Data_Generator"
  project_id     = var.project_id
  region         = var.region
}

module "streamlit" {
  source         = "./streamlit"

  project_id     = var.project_id
  region         = var.region
  artifact_repo  = var.artifact_repo
  image_name     = var.image_name
  depends_on = [module.data_generator]
}