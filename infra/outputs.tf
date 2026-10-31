output "instance_group" {
  value = google_compute_instance_group_manager.mirror.id
}

output "github_app_secret" {
  value = "${google_secret_manager_secret.github_app.id}/versions/latest"
}
