resource "yandex_vpc_network" "network" {
  name = var.settings.name
}

resource "yandex_vpc_subnet" "subnet" {
  name           = var.settings.subnet_name
  zone           = var.zone
  network_id     = yandex_vpc_network.network.id
  v4_cidr_blocks = [var.settings.subnet_range]
  route_table_id = yandex_vpc_route_table.route_table.id
}

resource "yandex_vpc_gateway" "nat_gateway" {
  name = var.settings.nat_gateway_name
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "route_table" {
  name       = var.settings.route_table_name
  network_id = yandex_vpc_network.network.id

  static_route {
    destination_prefix = var.settings.destination_prefix
    gateway_id         = yandex_vpc_gateway.nat_gateway.id
  }
}
