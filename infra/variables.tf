variable "image" {
  description = "Mirror container pinned by its Artifact Registry digest."
  type        = string
  validation {
    condition     = can(regex("^us-central1-docker\\.pkg\\.dev/kernelorg-mirror/lore-mirror/lore-mirror@sha256:[0-9a-f]{64}$", var.image))
    error_message = "Use the us-central1 Artifact Registry image pinned by SHA-256 digest."
  }
}

variable "enabled" {
  description = "Run one mirror VM when true; scale the group to zero when false."
  type        = bool
  default     = false
}

variable "lists" {
  description = "Comma-separated mailing lists; empty selects all lists."
  type        = string
  default     = ""
  validation {
    condition = var.lists == "" || alltrue([
      for name in split(",", var.lists) : can(regex("^[A-Za-z0-9][A-Za-z0-9._-]*$", name))
    ])
    error_message = "Use an empty string for all inboxes or comma-separated names. Each name must start with a letter or digit and contain only letters, digits, dots, underscores, or hyphens."
  }
}
