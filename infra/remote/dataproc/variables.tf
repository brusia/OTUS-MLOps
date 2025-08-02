variable "zone" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "cloud_auth" {
    type = object({
        # zone = string
        token = string
        cloud_id = string
        folder_id = string
        # service_account_name = string
        # security_group_name = string
    })
}

# variable "ssh_key" {
#     type = object({
#       public_key_path = string
#         private_key_path = string
#     })
# }

variable "network_settings" {
    type = object({
        name = string
        subnet_name = string
        subnet_range = string
        route_table_name = string
        nat_gateway_name = string
        # zone = string
        destination_prefix = string
        service_account_name = string
    })
}

# variable "virtual_machine" {
#     type = object({
#       name = string
#       instance_name = string
#       image_id = string
#       platform_id = string
#       user_name = string
#       cores = number
#       memory = number
#     })
# }

variable "storage_secrets" {
  type = object({
      access_key = string
      secret_key = string
    })
}

variable "git" {
  type = object({
    repo = string
    branch = string
    token = string
  })
}

variable "dataproc" {
      type = object({
        # instance_name = string
        # bucket = string
        # image_id = string
        version = string
        service_account_name = string

        # TODO: figure out how to make it dinamic
        master_resource = object({
            resource_preset_id = string
            disk_type_id       = string
            disk_size          = number
            hosts_count = number
            })

        # compute_resources = object({
        #     resource_preset_id = string
        #     disk_type_id       = string
        #     disk_size          = number
        #     hosts_count = number
        #     })

        data_resources = object({
          resource_preset_id = string
          disk_type_id       = string
          disk_size          = number
          hosts_count = number
          })
        })

        # default = {
        #   resources = {
        #   "dataproc_master_resources": {
        #     resource_preset_id = "s3-c4-m16"
        #     disk_type_id       = "network-ssd"
        #     disk_size          = 40
        #   }
        #   "dataproc_compute_resources": {
        #     resource_preset_id = "s3-c4-m16"
        #     disk_type_id       = "network-ssd"
        #     disk_size          = 50
        #   }
        #   "dataproc_data_resources": {
        #     resource_preset_id = "s3-c4-m16"
        #     disk_type_id       = "network-ssd"
        #     disk_size          = 50
        #   }
        # }
    # }
}