variable "zone" {
    type = string
}

variable "cloud_auth" {
    type = object({
        # zone = string
        token = string
        cloud_id = string
        folder_id = string
    })
}

variable "settings" {
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