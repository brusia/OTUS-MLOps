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
        service_account_name = string
        security_group_name = string
    })
}

variable "ssh_key" {
    type = object({
      public_key_path = string
        private_key_path = string
    })
}

variable "virtual_machine" {
    type = object({
      name = string
      instance_name = string
      image_id = string
      platform_id = string
      user_name = string
      cores = number
      memory = number
    })
}

variable "storage_secrets" {
  type = object({
      access_key = string
      secret_key = string
    })
}

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