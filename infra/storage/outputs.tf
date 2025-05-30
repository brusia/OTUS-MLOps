output "bucket_name" {
  description = "Name of Cloud Storage Bucket"
  value = yandex_storage_bucket.data_bucket.bucket
}

output "service_account_access_key" {
  description = "Access key for of Cloud Storage Service Account"
  value = yandex_iam_service_account_static_access_key.service-account-static-key.access_key
}

output "service_account_private_key" {
  description = "Access key for of Cloud Storage Service Account"
  value = yandex_iam_service_account_static_access_key.service-account-static-key.secret_key
}