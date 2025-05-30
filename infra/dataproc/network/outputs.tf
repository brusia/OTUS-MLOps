output "network_id" {
  description = "Network ID"
  value       = yandex_vpc_network.network.id
}

output "subnet_id" {
    description = "Subnet ID"
    value = yandex_vpc_subnet.subnet.id
}