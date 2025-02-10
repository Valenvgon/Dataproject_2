provider "google" {
    project = var.project_id
    region  = "europe-west1"
}

resource "google_pubsub_topic" "topic1" {
    name = "volunteers"
    project = var.project_id
}

resource "google_pubsub_topic" "topic2" {
    name = "affected"
    project = var.project_id
}
