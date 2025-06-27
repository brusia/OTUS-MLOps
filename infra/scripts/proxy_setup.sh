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

# Получаем ID мастер-ноды Dataproc кластера
log "Getting Dataproc master node ID"
DATAPROC_MASTER_FQDN=$(yc compute instance list --format json | jq -r '.[] | select(.labels.subcluster_role == "masternode") | .fqdn')

if [ -z "$DATAPROC_MASTER_FQDN" ]; then
    log "Failed to get master node ID"
    exit 1
fi

log "Master node FQDN: $DATAPROC_MASTER_FQDN"

# Создаем директорию .ssh и настраиваем приватный ключ
log "Creating .ssh directory and setting up private key"
mkdir -p $HOME/.ssh
echo "${private_key}" > $HOME/.ssh/dataproc_key
chmod 600 $HOME/.ssh/dataproc_key
chown ${user_name}:${user_name} $HOME/.ssh/dataproc_key

# Добавляем конфигурацию SSH для удобного подключения к мастер-ноде
log "Adding SSH configuration for master node connection"
cat <<EOF > $HOME/.ssh/config
Host dataproc-master
    HostName $DATAPROC_MASTER_FQDN
    User ${user_name}
    IdentityFile ~/.ssh/dataproc_key
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
EOF

chown ${user_name}:${user_name} $HOME/.ssh/config
chmod 600 $HOME/.ssh/config

# Настраиваем SSH-agent
log "Configuring SSH-agent"
eval $(ssh-agent -s)
echo "eval \$(ssh-agent -s)" >> $HOME/.bashrc
ssh-add $HOME/.ssh/dataproc_key
echo "ssh-add $HOME/.ssh/dataproc_key" >> $HOME/.bashrc

# Устанавливаем дополнительные полезные инструменты
log "Installing additional tools"
apt-get update
apt-get install -y tmux htop iotop

# Устанавливаем s3cmd
log "Installing s3cmd"
apt-get install -y s3cmd

# Настраиваем s3cmd
log "Configuring s3cmd"
cat <<EOF > $HOME/.s3cfg
[default]
access_key = ${access_key}
secret_key = ${secret_key}
host_base = storage.yandexcloud.net
host_bucket = %(bucket)s.storage.yandexcloud.net
use_https = True
EOF

chown ${user_name}:${user_name} $HOME/.s3cfg
chmod 600 $HOME/.s3cfg

# Определяем целевой бакет
TARGET_BUCKET=${s3_bucket}
SOURCE_BUCKET=otus-mlops-source-data

# Копируем данные из исходного бакета в новый (если они ещё не скопированы)
# На данный момент копирование происходит только в случае, если TARGET_BUCKET не содержит объектов.
# В дальнейшем имеет смысл проверять консистентность данных на SOURCE_BUCKET и TARGET_BUCKET (возможно, SOURCE_BUCKET будет обновляться и туда будут добавляться новые файлы или обновляться существующие)
# В этом случае нужно копировать только новые/изменённые данные
mapfile -t TARGET_BUCKET_OBJECTS < <(s3cmd ls --config=$${HOME}/.s3cfg "s3://$${TARGET_BUCKET}/data/raw/"| awk '{print $4}')

if [ $${#TARGET_BUCKET_OBJECTS[@]} -eq 0 ]; then
    log "There are no objects in target bucket. Copy data from source bucket to destination bucket"
    mapfile -t FILE_NAMES < <(s3cmd ls --config=$${HOME}/.s3cfg s3://$${SOURCE_BUCKET} | awk '{print $4}' | sed "s#s3://$${SOURCE_BUCKET}/##")
    for file_name in $${FILE_NAMES[@]}; do
        copy_file $${file_name} $${SOURCE_BUCKET} $${TARGET_BUCKET}/data/raw/ ${user_name}
    done
else
    log "Target bucket already contains objects."
    log "Listing contents of $${TARGET_BUCKET}/data/raw/"
    log "Objects count: $${#TARGET_BUCKET_OBJECTS[@]}"
    printf '%s\n' "$${TARGET_BUCKET_OBJECTS[@]}"
fi


# Создаем директорию для скриптов на прокси-машине
log "Creating scripts directory on proxy machine"
mkdir -p $HOME/scripts
GIT_REPO=${git_repo}
echo $GIT_REPO

# Копируем скрипт dataproc_setup_script.sh на прокси-машину
log "Copying dataproc_setup_script.sh script to proxy machine"
cat > $HOME/scripts/dataproc_setup_script.sh << 'EOL'
${dataproc_init_content}
EOL
cat $HOME/scripts/dataproc_setup_script.sh
sed -i "s/{{ s3_bucket }}/$${TARGET_BUCKET}\/data\/raw/g" $HOME/scripts/dataproc_setup_script.sh
cat $HOME/scripts/dataproc_setup_script.sh
sed -i "s/{{ git_repo }}/$${GIT_REPO}/g" $HOME/scripts/dataproc_setup_script.sh
cat $HOME/scripts/dataproc_setup_script.sh

# Устанавливаем правильные разрешения для скрипта на прокси-машине
log "Setting permissions for dataproc_setup_script.sh on proxy machine"
chmod +x $HOME/scripts/dataproc_setup_script.sh

# Проверяем подключение к мастер-ноде
DATAPROC_HOST=${user_name}'@'$DATAPROC_MASTER_FQDN
log "Checking connection to master node"
source $HOME/.bashrc
ssh -i $HOME/.ssh/dataproc_key -o StrictHostKeyChecking=no $DATAPROC_HOST "echo 'Connection successful'"
if [ $? -eq 0 ]; then
    log "Connection to master node successful"
else
    log "Failed to connect to master node"
    exit 1
fi


# Настраиваем переменные для работы с s3 на dataproc
# Настраиваем s3cmd
log "Configuring storage credentials"
ssh -i $HOME/.ssh/dataproc_key -o StrictHostKeyChecking=no $DATAPROC_HOST "echo 'export AWS_ACCESS_KEY_ID=${access_key}' >> .bashrc"
ssh -i $HOME/.ssh/dataproc_key -o StrictHostKeyChecking=no $DATAPROC_HOST "echo 'export AWS_SECRET_ACCESS_KEY=${secret_key}' >> .bashrc"
# ssh -i $HOME/.ssh/dataproc_key -o StrictHostKeyChecking=no $DATAPROC_HOST "source .bashrc"


# Копируем скрипт dataproc_setup_script.sh с прокси-машины на мастер-ноду
log "Create logs directory on dataproc master node"
ssh -i $HOME/.ssh/dataproc_key -o StrictHostKeyChecking=no $DATAPROC_HOST "mkdir -p $${HOME}/scripts"

log "Copy dataproc_setup_script.sh script to master node"
rsync -a $HOME/scripts/dataproc_setup_script.sh $DATAPROC_HOST:$HOME/scripts/setup_script.sh
if [ $? -eq 0 ]; then
    log "Copy succesfull"
else
    log "Failed to copy"
    exit 1
fi

# Устанавливаем правильные разрешения для скрипта на мастер-ноде
log "Setting permissions for dataproc_setup_script.sh on master node"
ssh -i $HOME/.ssh/dataproc_key -o StrictHostKeyChecking=no $DATAPROC_HOST "chmod +x $${HOME}/scripts/setup_script.sh"

# Копируем репо на proxy-машину
# rsync -a $HOME/scripts/repo $DATAPROC_HOST:$HOME/repo/otus-mlops

log "Running dataproc_setup_script on dataproc master node"
mkdir -p $HOME/logs
ssh -i $HOME/.ssh/dataproc_key -o StrictHostKeyChecking=no ubuntu@$DATAPROC_MASTER_FQDN "$${HOME}/scripts/setup_script.sh" > $HOME/logs/dataproc_master_execution.log 2>&1

log "dataproc_setup_script results"
cat $HOME/logs/dataproc_master_execution.log

# log "Upload results to storage bucket"
# current_time=$( date +'%Y-%m-%d_%H-%M-%S')
# s3cmd put --config=/home/${user_name}/.s3cfg --acl-public \
#     $HOME/logs/dataproc_master_execution.log \
#     s3://$TARGET_BUCKET/logs/$${current_time}_dataproc_master_execution.log

# Изменяем владельца лог-файла
log "Changing ownership of log file"
sudo chown ${user_name}:${user_name} $HOME/user_data_execution.log 

log "User data script execution completed"