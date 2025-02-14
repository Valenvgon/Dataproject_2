module "generator_data" {
  source         = "./modules/generator_data"

  project_id     = var.project_id
  region         = var.region
  artifact_repo  = var.artifact_repo
  image_name     = var.image_name
  affected_topic = var.affected_topic
  volunteer_topic= var.volunteer_topic
}
