
zone = "ru-central1-a"

# yandex cloud provider authentification data
cloud_auth = {
    token = "y0__xDPm_G1BBjB3RMg7577jhOSIFBDVRhlW46RFBHmpXYXif4-0Q"
    cloud_id = "brusiacloud"
    folder_id = "b1giq8pjuc1qoj6tmdc7"
    security_group_name = "brusia-security-group"
    service_account_name = "brusia-cloud-service-account-name"
}

ssh_key = {
    public_key_path = "/Users/avbrusina/.ssh/ya_cloud_rsa.pub"
    private_key_path = "/Users/avbrusina/.ssh/ya_cloud_rsa"
}

network_settings = {
  name = "brusia-bucket"
  subnet_name = "brusia-subnet"
  subnet_range = "10.0.0.0/24"
  nat_gateway_name = "brusia-nat-gateway"
  route_table_name = "brusia-route-table"
  destination_prefix = "0.0.0.0/0"
  service_account_name = "brusia-network-account-name"
}

storage = {
    #   buckets = ["brusia-mlops-bucket"]
    bucket = "brusia-mlops-bucket"
    service_account_name = "brusia-storage-service-account"
}

 # fd845dr9j4h2aaq1m6ko -- ubuntu 22.04
virtual_machine = {
  name = "brusia-boot-disk"
  instance_name = "brusia-proxy-vm"
  image_id = "fd808e721rc1vt7jkd0o"
  platform_id = "standard-v3"
  cores = 2
  memory = 16
  user_name = "ubuntu"  # do not change, chosen image has only ubuntu-user.
  # user_name = "brusia"
}

dataproc_settings = {
    version = "2.0"
    service_account_name = "brusia-dataproc-service-account"

    master_resource = {
            resource_preset_id = "s3-c2-m8"
            disk_type_id       = "network-ssd"
            disk_size          = 40
            hosts_count = 1 
            }

    data_resources = {
            resource_preset_id = "s3-c4-m16"
            disk_type_id = "network-hdd"  # network-sdd cound not been created: available compute.ssdDisks.size available is 214748364800.00
            disk_size = 128
            hosts_count = 3
            },
}
