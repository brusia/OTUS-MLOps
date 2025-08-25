#!/bin/bash

# Функция для логирования
function log() {
    sep="----------------------------------------------------------"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $sep " | tee -a $HOME/user_data_execution.log
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [INFO] $1" | tee -a $HOME/user_data_execution.log
}

log "Starting user data script execution"
log "VM user name: ${user_name}"

# Устанавливаем yc CLI
log "Installing yc CLI"
export HOME="/home/${user_name}"
curl https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash

# Изменяем владельца директории yandex-cloud и её содержимого
log "Changing ownership of yandex-cloud directory"
sudo chown -R ${user_name}:${user_name} $HOME/yandex-cloud

# Применяем изменения из .bashrc
log "Applying changes from .bashrc"
source $HOME/.bashrc

# Проверяем, что yc доступен
if command -v yc &> /dev/null; then
    log "yc CLI is now available"
    yc --version
else
    log "yc CLI is still not available. Adding it to PATH manually"
    export PATH="$${PATH}:$HOME/yandex-cloud/bin"
    yc --version
fi

# Настраиваем yc CLI
log "Configuring yc CLI"
yc config set token ${token}
yc config set cloud-id ${cloud_id}
yc config set folder-id ${folder_id}

# Устанавливаем jq
log "Installing jq"
sudo apt-get update
sudo apt-get install -y jq

# Устанавливаем и настраиваем docker
sudo apt-get install docker.io -y
sudo apt-get install docker-compose-v2
sudo usermod -aG docker ${user_name}
newgrp docker

# Устанавливаем переменные окружения для DAG-ов
echo 'export S3_ACCESS_KEY="${access_key}"' >> $HOME/.bashrc
echo 'export S3_SECRET_KEY="${secret_key}"' >> $HOME/.bashrc
echo 'export S3_BUCKET_NAME="${s3_bucket}"' >> $HOME/.bashrc

# source $HOME/.bashrc

# Поднимаем всю инфораструктуру из docker-compose
log "Downloading docker-compose file"
cd $HOME
echo '${docker_compose_content}' > /home/ubuntu/docker-compose.yaml

log "Setting up infrastructure"
mkdir -p ./dags ./logs ./plugins ./config
chown ${user_name}:${user_name} ./dags ./logs ./plugins ./config docker-compose.yaml
echo -e "AIRFLOW_UID=$(id -u)" >> .env
echo -e "S3_ACCESS_KEY=${access_key}" >> .env
echo -e "S3_SECRET_KEY=${secret_key}" >> .env
echo -e "S3_BUCKET_NAME=${s3_bucket}" >> .env
echo -e "S3_ENDPOINT_URL=https://storage.yandexcloud.net" >> .env
echo -e "MLFLOW_S3_ENDPOINT_URL=${S3_ENDPOINT_URL}" >> .env
echo -e "MLFLOW_TRACKING_URI=http://127.0.0.1:5000" >> .env
echo -e "AWS_ACCESS_KEY_ID=${S3_ACCESS_KEY}" >> .env
echo -e "AWS_SECRET_ACCESS_KEY=${S3_SECRET_KEY}" >> .env

AIRFLOW_UID=50000

source .env
docker compose up &
