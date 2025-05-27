
# Service accounts resources
resource "yandex_iam_service_account" "service-account" {
  name        = var.service_account_name
  description = "Service account for Storage"
}

resource "yandex_resourcemanager_folder_iam_member" "service-account-roles" {
  for_each = toset([
    "storage.admin",
    "iam.serviceAccounts.user",
    "storage.uploader",
    "storage.viewer",
    "storage.editor"
  ])

  folder_id = var.cloud_auth.folder_id
  role      = each.key
  member    = "serviceAccount:${yandex_iam_service_account.service-account.id}"
}

resource "yandex_iam_service_account_static_access_key" "service-account-static-key" {
  service_account_id = yandex_iam_service_account.service-account.id
  description        = "Static access key for object storage"
}

# Storage bucket resource
resource "yandex_storage_bucket" "data_bucket" {
  depends_on = [yandex_resourcemanager_folder_iam_member.service-account-roles]
  bucket        = "${var.bucket_name}"
  access_key    = yandex_iam_service_account_static_access_key.service-account-static-key.access_key
  secret_key    = yandex_iam_service_account_static_access_key.service-account-static-key.secret_key
  acl    = "public-read"
  force_destroy = true
}