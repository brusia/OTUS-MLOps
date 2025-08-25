resource "yandex_iam_service_account" "service-account" {
  name        = var.cloud_auth.service_account_name
  description = "Service account for Dataproc cluster and related services"
}

resource "yandex_resourcemanager_folder_iam_member" "service-account-roles" {
  for_each = toset([
    "managed-airflow.integrationProvider",
    "managed-airflow.admin",
    "dataproc.editor",
    "dataproc.agent",
    "mdb.dataproc.agent",
    "vpc.user",
    "iam.serviceAccounts.user",
    "compute.admin",
    "storage.admin",
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
  description        = "Static access key for data proc"
}


resource "yandex_compute_disk" "boot_disk" {
  name     = var.virtual_machine.name
  zone     = var.zone
  image_id = var.virtual_machine.image_id
  size     = 30
}

resource "local_file" "sa_static_key_file" {
  content = jsonencode({
    id                 = yandex_iam_service_account_static_access_key.service-account-static-key.id
    service_account_id = yandex_iam_service_account_static_access_key.service-account-static-key.service_account_id
    created_at         = yandex_iam_service_account_static_access_key.service-account-static-key.created_at
    s3_key_id          = yandex_iam_service_account_static_access_key.service-account-static-key.access_key
    s3_secret_key      = yandex_iam_service_account_static_access_key.service-account-static-key.secret_key
  })
  filename        = "${path.module}/static_key.json"
  file_permission = "0600"
}

resource "yandex_compute_instance" "proxy" {
  name                      = var.virtual_machine.instance_name
  allow_stopping_for_update = true
  platform_id               = var.virtual_machine.platform_id
  zone                      = var.zone
  service_account_id        = yandex_iam_service_account.service-account.id

  metadata = {
    # ssh-keys = "${var.virtual_machine.user_name}:${file(var.ssh_key.public_key_path)}"
    ssh-keys = "ubuntu:${file(var.ssh_key.public_key_path)}"
    user-data = templatefile("${path.root}/scripts/proxy_setup.sh", {
      token                       = var.cloud_auth.token
      cloud_id                    = var.cloud_auth.cloud_id
      folder_id                   = var.cloud_auth.folder_id
      private_key                 = file(var.ssh_key.private_key_path)
      access_key                  = var.storage_secrets.access_key
      secret_key                  = var.storage_secrets.secret_key
      s3_bucket                   = var.bucket_name
      docker_compose_content = file("${path.root}/infra/src/docker-compose.yaml")
      # user_name                   = var.virtual_machine.user_name
      user_name = "ubuntu"
    #   dataproc_init_content = file("${path.root}/scripts/dataproc_setup_script.sh")
      # git_user                    = var.git_user
    #   git_repo                    = var.git.repo
    #   git_branch = var.git.branch
    #   git_token                   = var.git.token
    })
  }

  scheduling_policy {
    preemptible = true
  }

  resources {
    cores  = var.virtual_machine.cores
    memory = var.virtual_machine.memory
  }

  boot_disk {
    disk_id = yandex_compute_disk.boot_disk.id
  }

  network_interface {
    subnet_id = module.network.subnet_id
    nat       = true
  }

  metadata_options {
    gce_http_endpoint = 1
    gce_http_token    = 1
  }

  connection {
    type        = "ssh"
    user = "ubuntu"
    # user        = var.virtual_machine.user_name
    private_key = file(var.ssh_key.private_key_path)
    host        = self.network_interface[0].nat_ip_address
  }

  provisioner "remote-exec" {
    inline = [
      "sudo cloud-init status --wait",
      "echo 'User-data script execution log:' | sudo tee /var/log/user_data_execution.log",
      "sudo cat /var/log/cloud-init-output.log | sudo tee -a /var/log/user_data_execution.log",
    ]
  }
  depends_on = [module.network]
}

module "network" {
   source = "./network"
   providers = { yandex = yandex }
   zone = var.zone
   cloud_auth = var.cloud_auth
   settings = var.network_settings
}