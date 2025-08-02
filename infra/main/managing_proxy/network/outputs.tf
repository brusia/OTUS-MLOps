output "network_id" {
  description = "Network ID"
  value       = yandex_vpc_network.network.id
}

output "subnet_id" {
    description = "Subnet ID"
    value = yandex_vpc_subnet.subnet.id
}

output "security_group_id" {
    description = "Security Group ID"
    value = yandex_vpc_security_group.security_group.id
}