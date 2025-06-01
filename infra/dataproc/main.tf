
module "network" {
    source = "./network"
    providers = { yandex = yandex }

    zone = var.zone
    cloud_auth = var.cloud_auth
    settings = { 
        name = var.network_settings.name
        subnet_name = var.network_settings.subnet_name
        subnet_range = var.network_settings.subnet_range
        route_table_name = var.network_settings.route_table_name
        nat_gateway_name = var.network_settings.nat_gateway_name
        # zone = var.cloud_auth.zone  # may very from cloud_auth, this is only an example
        destination_prefix = var.network_settings.destination_prefix
        service_account_name = var.network_settings.service_account_name
    }
}

resource "yandex_iam_service_account" "service-account" {
  name        = var.dataproc.service_account_name
  description = "Service account for Dataproc cluster and related services"
}

resource "yandex_resourcemanager_folder_iam_member" "service-account-roles" {
  for_each = toset([
    "dataproc.editor",
    "dataproc.agent",
    "compute.admin",
    "vpc.user",
    "iam.serviceAccounts.user",
  ])

  folder_id = var.cloud_auth.folder_id
  role      = each.key
  member    = "serviceAccount:${yandex_iam_service_account.service-account.id}"
}

resource "yandex_iam_service_account_static_access_key" "service-account-static-key" {
  service_account_id = yandex_iam_service_account.service-account.id
  description        = "Static access key for data proc"
}

resource "yandex_vpc_security_group" "security_group" {
  name        = var.cloud_auth.security_group_name
  description = "Security group for Dataproc cluster"
  network_id  = module.network.network_id

  ingress {
    protocol       = "ANY"
    description    = "Allow all incoming traffic"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    protocol       = "TCP"
    description    = "UI"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 443
  }

  ingress {
    protocol       = "TCP"
    description    = "SSH"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 22
  }

#   Nobody needs Jupyter and we don’t either!
#  <there was a removed block for Jupyter here>

  egress {
    protocol       = "ANY"
    description    = "Allow all outgoing traffic"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol       = "ANY"
    description    = "Allow all outgoing traffic"
    from_port      = 0
    to_port        = 65535
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol          = "ANY"
    description       = "Internal"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }
}


# Dataproc resources
resource "yandex_dataproc_cluster" "dataproc_cluster" {
  depends_on  = [yandex_resourcemanager_folder_iam_member.service-account-roles]
  bucket      = var.bucket_name
  description = "Dataproc Cluster created by Terraform for OTUS project"
  name        = var.virtual_machine.instance_name
  labels = {
    created_by = "terraform"
  }
  service_account_id = yandex_iam_service_account.service-account.id
  zone_id            = var.zone
  security_group_ids = [yandex_vpc_security_group.security_group.id]


  cluster_config {
    version_id = var.dataproc.version

    hadoop {
      services = ["HDFS", "YARN", "SPARK", "TEZ", "HIVE"]
      properties = {
        "yarn:yarn.resourcemanager.am.max-attempts" = 5
      }
      ssh_public_keys = [file(var.ssh_key.public_key_path)]
    }

    # TODO: dinamic instantiate from cfg
    subcluster_spec {
      name = "master"
      role = "MASTERNODE"
      resources {
        resource_preset_id = var.dataproc.master_resource.resource_preset_id
        disk_type_id       = var.dataproc.master_resource.disk_type_id
        disk_size          = var.dataproc.master_resource.disk_size
      }
      subnet_id        = module.network.subnet_id
      hosts_count      = var.dataproc.master_resource.hosts_count
      assign_public_ip = true
    }

    subcluster_spec {
      name = "data"
      role = "DATANODE"
      resources {
        resource_preset_id = var.dataproc.data_resources.resource_preset_id
        disk_type_id       = var.dataproc.data_resources.disk_type_id
        disk_size          = var.dataproc.data_resources.disk_size
      }
      subnet_id   = module.network.subnet_id
      hosts_count = var.dataproc.data_resources.hosts_count
    }

    # subcluster_spec {
    #   name = "compute"
    #   role = "COMPUTENODE"
    #   resources {
    #     resource_preset_id = var.dataproc_compute_resources.resource_preset_id
    #     disk_type_id       = "network-ssd"
    #     disk_size          = var.dataproc_compute_resources.disk_size
    #   }
    #   subnet_id   = module.network.subnet_id
    #   hosts_count = 1
    # }
  }
}

# Compute resources
resource "yandex_compute_disk" "boot_disk" {
  name     = var.virtual_machine.name
  zone     = var.zone
  image_id = var.virtual_machine.image_id
  size     = 30
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
      # user_name                   = var.virtual_machine.user_name
      user_name = "ubuntu"
      dataproc_init_content = file("${path.root}/scripts/dataproc_setup_script.sh")
      # git_user                    = var.git_user
      git_repo                    = var.git.repo
      git_token                   = var.git.token
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
  depends_on = [yandex_dataproc_cluster.dataproc_cluster]
}