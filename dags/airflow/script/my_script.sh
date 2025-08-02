#!/bin/bash

# Устанавливаем yc CLI
echo "Installing yc CLI"
curl https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash

# Изменяем владельца директории yandex-cloud и её содержимого
echo "Changing ownership of yandex-cloud directory"
sudo chown -R ${USER}:${USER} $HOME/yandex-cloud $HOME/.config

# Проверяем, что yc доступен
if command -v yc &> /dev/null; then
    echo "yc CLI is now available"
    yc --version
else
    echo "yc CLI is still not available. Adding it to PATH manually"
    export PATH="${PATH}:$HOME/yandex-cloud/bin"
    yc --version
fi

# Настраиваем yc CLI
echo "Configuring yc CLI"
yc config set token ${YC_TOKEN}
yc config set cloud-id ${YC_CLOUD_ID}
yc config set folder-id ${YC_FOLDER_ID}

# # Устанавливаем jq
# log "Installing jq"
# sudo apt-get update
# sudo apt-get install -y jq

# Устанавливаем s3cmd
# log "Installing s3cmd"
# apt-get install -y s3cmd

# Настраиваем s3cmd
# log "Configuring s3cmd"
# mkdir -p /tmp
# touch /tmp/.s3cfg
# cat <<EOF > /tmp/.s3cfg
# [default]
# access_key = ${S3_ACCESS_KEY}
# secret_key = ${S3_SECRET_KEY}
# host_base = storage.yandexcloud.net
# host_bucket = %(bucket)s.storage.yandexcloud.net
# use_https = True
# EOF

# chmod 666 /tmp/.s3cfg

# s3cmd ls --config=/tmp/.s3cfg "s3://${S3_BUCKET_NAME}/data/raw/"

yc dataproc cluster delete tmp-spark