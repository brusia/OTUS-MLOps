variable "zone" {
    type = string
}

variable "cloud_auth" {
    type = object({
        token = string
        cloud_id = string
        folder_id = string
    })
}

variable "service_account_name" {
  type        = string
  description = "Name of the service account"
  default     = "brusia-storage-service-account"
}

variable "bucket_name" {
  type        = string
  description = "Name of the bucket"
  default = "brusia-bucket"
}