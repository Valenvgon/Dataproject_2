module "data_generator" {
  source         = "./modules/Data_Generator"
  project_id     = var.project_id
  region         = var.region
}

module "dataflow" {
  source         = "./modules/Dataflow"
  project_id     = var.project_id
  region         = var.region
  bucket_dataflow   = var.bucket_dataflow
}