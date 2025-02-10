output "artifact_repo" {
  value = google_artifact_registry_repository.repo.id
}

output "image_url" {
  value = "europe-west1-docker.pkg.dev/${google_artifact_registry_repository.repo.project}/${google_artifact_registry_repository.repo.repository_id}/generator-image:latest"
}