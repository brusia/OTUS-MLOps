#!/bin/bash

# Функция логирования
function log() {
    sep="----------------------------------------------------------"
    echo "[$(date)] $sep "
    echo "[$(date)] [INFO] $1"
}

# Проверяем, передан ли аргумент (имя файла)
if [ -n "$1" ]; then
    FILE_NAME="$1"
    log "File name provided: $FILE_NAME"
else
    log "No file name provided, copying all files"
fi

# Создаем директорию в HDFS
log "Creating directory in HDFS"
hdfs dfs -mkdir -p /user/ubuntu/data

# Копируем данные из S3 в зависимости от того, передано ли имя файла
if [ -n "$FILE_NAME" ]; then
    # Копируем конкретный файл
    log "Copying specific file from S3 to HDFS"
    hadoop distcp s3a://{{ s3_bucket }}/$FILE_NAME user/ubuntu/data/$FILE_NAME
else
    # Копируем все данные
    log "Copying all data from S3 to HDFS"
    hadoop distcp s3a://{{ s3_bucket }}/2019-08-22.txt /user/ubuntu/data/2019-08-22.txt
    # hadoop distcp s3a://{{ s3_bucket }}/2020-05-18.txt /user/ubuntu/data/2020-05-18.txt
    # hadoop distcp s3a://{{ s3_bucket }}/2022-05-08.txt /user/ubuntu/data/2022-05-08.txt
    # hadoop distcp s3a://{{ s3_bucket }}/* /user/ubuntu/data
fi

# Выводим содержимое директории для проверки
# log "Listing files in HDFS directory"

# Проверяем успешность выполнения операции
# if hdfs dfs -ls /user/ubuntu/data
# then
#     log "Data was successfully copied to HDFS"
# else
#     log "Failed to copy data to HDFS"
#     exit 1
# fi


# Временное решение. В дальнейшем в локальной сети dataproc-кластера будет развёрнут артефакторий. Из артефактория dataproc кластер будет устанавливать python-пакет. Пока же мы этот пакет собираем из репозитория.
# Клонируем репозиторий github на data-proc-кластер

pip install uv
git clone {{ git_repo }}
cd OTUS-MLOps
git checkout hometask_4
# python3 -m uv venv --system-site-packages
# python3 -m uv sync --group data-analyse
# python3 -m uv pip install -e .

uv venv --system-site-packages
uv sync --group data-analyse
uv pip install -e .

# запускаем скрипт анализа и очистки данных на удалённой машине
# для лучше управляемости до настройки запуска по расписанию скрипт анализа и очистки данных
# лучше запускать мануально, используя утилиты контроля сессии (например, tmux), либо запускать выполнение в фоновом режиме
# Рекомендация дана с целью избежать прерывания сессии при работе на удалённой ноде data-proc-master
# uv run src/otus_mlops/scripts/analyse_data.py