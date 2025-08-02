module "storage" {
  source = "./storage"
  zone = var.zone
  cloud_auth = var.cloud_auth
  providers = { yandex = yandex }
}

module "managing_proxy" {
   providers = { yandex = yandex }
   source = "./managing_proxy"
   zone = var.zone
   bucket_name = var.bucket_name
   cloud_auth = var.cloud_auth
   ssh_key = var.ssh_key
   virtual_machine = var.virtual_machine
   storage_secrets = { access_key = module.storage.service_account_access_key, secret_key = module.storage.service_account_private_key }
   network_settings = var.network_settings
}