variable "image" {
  description = "Mirror container pinned by its Artifact Registry digest."
  type        = string
  validation {
    condition     = can(regex("^us-central1-docker\\.pkg\\.dev/kernelorg-mirror/lore-mirror/lore-mirror@sha256:[0-9a-f]{64}$", var.image))
    error_message = "Use the us-central1 Artifact Registry image pinned by SHA-256 digest."
  }
}

variable "enabled" {
  description = "Start mirroring after GitHub App credentials have been stored."
  type        = bool
  default     = false
}

variable "lists" {
  description = "Comma-separated mailing lists; empty selects all lists."
  type        = string
  default     = ""
  validation {
    condition     = can(regex("^[A-Za-z0-9._,-]*$", var.lists))
    error_message = "List names may contain letters, numbers, dots, underscores and hyphens, separated by commas."
  }
}
