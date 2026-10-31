output "vm" {
  value = google_compute_instance.mirror.id
}

output "image" {
  value = var.image
}

output "github_app_secret" {
  value = "${google_secret_manager_secret.github_app.id}/versions/latest"
}

output "build_service_account" {
  value = google_service_account.build.email
}

output "source_bucket" {
  value = google_storage_bucket.source.url
}
