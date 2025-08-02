#!/bin/bash

# Функция для логирования
function log() {
    sep="----------------------------------------------------------"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $sep " | tee -a $HOME/user_data_execution.log
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [INFO] $1" | tee -a $HOME/user_data_execution.log
}

# Функция для копирования заданного файла в целевой бакет
function copy_file() {
    FILE_NAME=$1
    SOURCE=$2
    TARGET=$3
    USER_NAME=$4
    s3cmd cp \
        --config=/home/$USER_NAME/.s3cfg \
        --acl-public \
        s3://$SOURCE/$FILE_NAME \
        s3://$TARGET/$FILE_NAME

    # Проверяем успешность копирования
    if [ $? -eq 0 ]; then
        log "Listing contents of $TARGET"
        s3cmd ls --config=/home/$USER_NAME/.s3cfg s3://$TARGET/
    else
        log "Error occurred while copying file "$FILE_NAME" to "$TARGET
    fi
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

# Поднимаем apache airflow сервер
# TODO: здесь можно будет поменять на свой docker-compose, выкинуть лишние сервисы БД либо переконфигурировать в соответствии с нашими требованиями, а скачивать по ссылке с s3 (передавать как аргумент terraform при инициализации)
log "Downloading docker-compose file"
cd $HOME
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.3/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins ./config
chown ${user_name}:${user_name} ./dags ./logs ./plugins ./config docker-compose.yaml
echo -e "AIRFLOW_UID=$(id -u)" >> .env
echo -e "S3_ACCESS_KEY=${access_key}" >> .env
echo -e "S3_SECRET_KEY=${secret_key}" >> .env
echo -e "S3_BUCKET_NAME=${s3_bucket}" >> .env
AIRFLOW_UID=50000
docker compose up &

source $HOME/.bashrc

# docker compose run airflow-cli airflow

log "I'm here. I'm done."

# TODO: здесь нужно создать airflow
# скрипты


# настроить его расписание

# а он уже сам пусть создаёт из terraform наш dataproc

# дальше был код, теперь он пусть исполняется из airflow (первого шага)