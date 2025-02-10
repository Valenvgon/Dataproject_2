module "pubsub" {
  source = "./modules/Data_generator"
  project_id = var.project_id
}

module "Dataflow" {
  source = "./modules/Dataflow"
  project_id = var.project_id
}