provider "yandex" {
  zone = var.zone
  token = var.cloud_auth.token
  cloud_id = var.cloud_auth.cloud_id
  folder_id = var.cloud_auth.folder_id
}