resource "google_storage_bucket" "dataflow_bucket" {
  name     = "${var.project_id}-dataflow-bucket"
  location = var.region

  storage_class = "STANDARD"

  uniform_bucket_level_access = true
}

resource "google_pubsub_topic" "matched" {
  name = "matched"
}

resource "google_pubsub_topic" "not_matched" {
  name = "not_matched"
}

resource "google_bigquery_dataset" "volunteer_data" {
  dataset_id = "volunteer_data"
  project    = var.project_id
  location   = var.region
}

resource "google_bigquery_table" "matches" {
  dataset_id = google_bigquery_dataset.volunteer_data.dataset_id
  table_id   = "matched_volunteers"

  schema = <<EOF
[
  {"name": "volunteer_id", "type": "STRING", "mode": "REQUIRED"},
  {"name": "affected_id", "type": "STRING", "mode": "REQUIRED"},
  {"name": "match_score", "type": "FLOAT", "mode": "REQUIRED"}
]
EOF
}