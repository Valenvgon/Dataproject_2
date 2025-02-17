module "generator_data" {
  source                      = "./modules/generator_data"
  project_id                  = var.project_id
  region                      = var.region
  artifact_repo_generator     = var.artifact_repo_generator
  image_name                  = var.image_name
  affected_topic              = var.affected_topic
  volunteer_topic             = var.volunteer_topic
}

module "dataflow_data" {
  source                      = "./modules/dataflow"
  project_id                  = var.project_id
  region                      = var.region
  artifact_repo_dataflow      = var.artifact_repo_dataflow
  image_name_dflow            = var.image_name_dflow
  bucket_dataflow             = var.bucket_dataflow
  bq_dataset                  = var.bq_dataset
  matched_table               = var.matched_table
  unmatched_table             = var.unmatched_table
  affected_topic              = var.affected_topic
  volunteer_topic             = var.volunteer_topic
  affected_sub                = var.affected_sub
  volunteer_sub               = var.volunteer_sub
  temp_location               = var.temp_location
  staging_location            = var.staging_location
}


module "streamlit" {
  source                      = "./modules/streamlit"
  project_id                  = var.project_id
  region                      = var.region
  artifact_repo_streamlit     = var.artifact_repo_streamlit
  image_name_streamlit        = var.image_name_streamlit
  affected_topic              = var.affected_topic
  volunteer_topic             = var.volunteer_topic
  bq_dataset_streamlit        = var.bq_dataset_streamlit
  bq_table_streamlit          = var.bq_table_streamlit
  bq_table_streamlit_matched  = var.bq_table_streamlit_matched
}