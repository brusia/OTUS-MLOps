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

yc dataproc cluster delete tmp-spark