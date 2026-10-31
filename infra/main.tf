terraform {
  required_version = "~> 1.5.7"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "8.9.0"
    }
  }
}

provider "google" {
  project                         = "kernelorg-mirror"
  region                          = "us-central1"
  zone                            = "us-central1-a"
  add_terraform_attribution_label = false
}

resource "google_project_service" "api" {
  for_each = toset([
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "iap.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "oslogin.googleapis.com",
    "secretmanager.googleapis.com",
    "telemetry.googleapis.com",
  ])
  project            = "kernelorg-mirror"
  service            = each.key
  disable_on_destroy = false
}

resource "google_service_account" "runtime" {
  account_id   = "lore-mirror"
  display_name = "lore-mirror"
  depends_on   = [google_project_service.api]
}

resource "google_service_account" "build" {
  account_id   = "lore-build"
  display_name = "lore-build"
  depends_on   = [google_project_service.api]
}

resource "google_project_iam_member" "logs_runtime" {
  project = "kernelorg-mirror"
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.runtime.email}"
}

resource "google_project_iam_member" "logs_build" {
  project = "kernelorg-mirror"
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.build.email}"
}

resource "google_project_iam_member" "telemetry" {
  for_each = toset([
    "roles/telemetry.writer",
    "roles/serviceusage.serviceUsageConsumer",
  ])
  project = "kernelorg-mirror"
  role    = each.key
  member  = "serviceAccount:${google_service_account.runtime.email}"
}

resource "google_artifact_registry_repository" "images" {
  location      = "us-central1"
  repository_id = "lore-mirror"
  description   = "Lore mirror images"
  format        = "DOCKER"
  docker_config {
    immutable_tags = true
  }
  depends_on = [google_project_service.api]
  lifecycle {
    prevent_destroy = true
  }
}

resource "google_artifact_registry_repository_iam_member" "runtime" {
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.runtime.email}"
}

resource "google_artifact_registry_repository_iam_member" "build" {
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.build.email}"
}

resource "google_secret_manager_secret" "github_app" {
  secret_id = "github-app"
  replication {
    user_managed {
      replicas {
        location = "us-central1"
      }
    }
  }
  depends_on = [google_project_service.api]
  lifecycle {
    prevent_destroy = true
  }
}

resource "google_secret_manager_secret_iam_member" "runtime" {
  secret_id = google_secret_manager_secret.github_app.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.runtime.email}"
}

resource "google_storage_bucket" "source" {
  name                        = "kernelorg-mirror-build-source"
  location                    = "US-CENTRAL1"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  soft_delete_policy {
    retention_duration_seconds = 0
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 7
    }
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "google_storage_bucket_iam_member" "build_source" {
  bucket = google_storage_bucket.source.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.build.email}"
}

resource "google_compute_network" "mirror" {
  name                    = "lore-mirror"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.api]
}

resource "google_compute_subnetwork" "mirror" {
  name                     = "lore-mirror"
  region                   = "us-central1"
  ip_cidr_range            = "10.42.0.0/24"
  network                  = google_compute_network.mirror.id
  private_ip_google_access = true
}

resource "google_compute_firewall" "iap_ssh" {
  name    = "lore-mirror-iap-ssh"
  network = google_compute_network.mirror.name
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges           = ["35.235.240.0/20"]
  target_service_accounts = [google_service_account.runtime.email]
}

resource "google_compute_firewall" "health" {
  name    = "lore-mirror-health"
  network = google_compute_network.mirror.name
  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }
  source_ranges           = ["130.211.0.0/22", "35.191.0.0/16"]
  target_service_accounts = [google_service_account.runtime.email]
}

resource "google_compute_health_check" "mirror" {
  name                = "lore-mirror"
  check_interval_sec  = 30
  timeout_sec         = 5
  healthy_threshold   = 1
  unhealthy_threshold = 3
  http_health_check {
    port         = 8080
    request_path = "/healthz"
  }
  log_config {
    enable = true
  }
  depends_on = [google_project_service.api]
}

resource "google_compute_instance_template" "mirror" {
  name_prefix  = "lore-mirror-"
  machine_type = "e2-medium"
  labels       = { app = "lore-mirror" }
  disk {
    boot         = true
    auto_delete  = true
    source_image = "projects/cos-cloud/global/images/cos-stable-121-18867-652-7"
    disk_size_gb = 20
    disk_type    = "pd-standard"
  }
  network_interface {
    subnetwork = google_compute_subnetwork.mirror.id
    access_config {
      network_tier = "STANDARD"
    }
  }
  service_account {
    email  = google_service_account.runtime.email
    scopes = ["cloud-platform"]
  }
  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }
  metadata = {
    "enable-oslogin"         = "TRUE"
    "block-project-ssh-keys" = "TRUE"
    "lore-mirror-image"      = var.image
    "lore-mirror-lists"      = var.lists
    "startup-script"         = file("${path.module}/startup.sh")
  }
  depends_on = [
    google_artifact_registry_repository_iam_member.runtime,
    google_secret_manager_secret_iam_member.runtime,
    google_project_iam_member.logs_runtime,
    google_project_iam_member.telemetry,
  ]
  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_instance_group_manager" "mirror" {
  name                      = "lore-mirror"
  base_instance_name        = "lore-mirror"
  zone                      = "us-central1-a"
  target_size               = var.enabled ? 1 : 0
  wait_for_instances        = true
  wait_for_instances_status = "UPDATED"
  version {
    instance_template = google_compute_instance_template.mirror.self_link_unique
  }
  auto_healing_policies {
    health_check      = google_compute_health_check.mirror.id
    initial_delay_sec = 300
  }
  update_policy {
    type                  = "PROACTIVE"
    minimal_action        = "REPLACE"
    max_surge_fixed       = 0
    max_unavailable_fixed = 1
    replacement_method    = "RECREATE"
  }
  depends_on = [google_compute_firewall.health]
}
