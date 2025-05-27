module "storage" {
  source = "./storage"
  zone = var.zone
  cloud_auth = var.cloud_auth
  providers = { yandex = yandex }
}

module "dataproc" {
  depends_on = [ module.storage ]
  providers = { yandex = yandex }

  source = "./dataproc"
  zone = var.zone
  bucket_name = module.storage.bucket_name
  cloud_auth = var.cloud_auth
  virtual_machine = var.virtual_machine
  network_settings = var.network_settings
  dataproc = var.dataproc_settings
  ssh_key = var.ssh_key
  storage_secrets = { access_key = module.storage.service_account_access_key, secret_key = module.storage.service_account_private_key }
}