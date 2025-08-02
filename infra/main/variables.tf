variable "zone" {
    type = string
}

variable "bucket_name" {
  type        = string
  description = "Name of the bucket"
  default = "brusia-bucket"
}

# yandex cloud provider authentification data
variable "cloud_auth" {
  type = object({
    token = string
    cloud_id = string
    folder_id = string

    service_account_name = string
    security_group_name = string
  })

    description = <<EOT
    cloud_auth = {
      cloud_id: "description"
      token = "description"
      folder_id = "description""
      zone = "description"
      service_account_name = "description"
      security_group_name = "description"
    }
  EOT
}

variable "ssh_key" {
    type = object({
      public_key_path = string
        private_key_path = string
    })
}

variable "storage" {
    type = object({
    #   buckets = list[string] -- may be list if several buckets are needed
      bucket = string
      service_account_name = string
    })

    default = {
    #   buckets = ["brusia-mlops-bucket"]
      bucket = "brusia-mlops-bucket"
      service_account_name = "brusia-mlops-storage-service-account"
    }
}

variable "network_settings" {
    type = object({
        name = string
        # zone = string
        subnet_name = string
        subnet_range = string
        route_table_name = string
        nat_gateway_name = string
        destination_prefix = string
        service_account_name = string
    })

    default = {
      name = "brusia-mlops-network"
      subnet_name = "brusina-mlops-subnet"
      subnet_range = "10.0.0.0/24"
      route_table_name = "brusia-mlops-route-table"
      nat_gateway_name = "brusia-mlops-nat-gateway"
      destination_prefix = "0.0.0.0/0"
      service_account_name = "brusia-mlops-network-service-account"
    }
}

variable "virtual_machine" {
    type = object({
      name = string
      instance_name = string
      image_id = string
      platform_id = string
      cores = number
      memory = number
      user_name = string
    })
}

variable "git" {
  type = object({
    repo = string
    branch = string
    token = string
  })
}
