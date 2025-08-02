
# module "network" {
#     source = "./network"
#     providers = { yandex = yandex }

#     zone = var.zone
#     cloud_auth = var.cloud_auth
#     settings = { 
#         name = var.network_settings.name
#         subnet_name = var.network_settings.subnet_name
#         subnet_range = var.network_settings.subnet_range
#         route_table_name = var.network_settings.route_table_name
#         nat_gateway_name = var.network_settings.nat_gateway_name
#         # zone = var.cloud_auth.zone  # may very from cloud_auth, this is only an example
#         destination_prefix = var.network_settings.destination_prefix
#         service_account_name = var.network_settings.service_account_name
#     }
# }

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

    # TODO: dynamic instantiate from cfg
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
    #     resource_preset_id = var.dataproc.compute_resources.resource_preset_id
    #     disk_type_id       = var.dataproc.compute_resources.disk_type_id
    #     disk_size          = var.dataproc.compute_resources.disk_size
    #   }
    #   subnet_id   = module.network.subnet_id
    #   hosts_count = var.dataproc.compute_resources.hosts_count
    # }
  }
}