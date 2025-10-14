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

# Поднимаем вспомогательные сервисы из docker-compose
log "Downloading docker-compose file"
cd $HOME
echo '${docker_compose_content}' > /home/ubuntu/docker-compose.yaml

log "Setting up infrastructure"
# airflow теперь живёт в kubernetes
# mkdir -p ./dags ./logs ./plugins ./config
# chown ${user_name}:${user_name} ./dags ./logs ./plugins ./config docker-compose.yaml
# echo -e "AIRFLOW_UID=$(id -u)" >> .env
echo -e "S3_ACCESS_KEY=${access_key}" >> .env
echo -e "S3_SECRET_KEY=${secret_key}" >> .env
echo -e "S3_BUCKET_NAME=${s3_bucket}" >> .env
echo -e "S3_ENDPOINT_URL=https://storage.yandexcloud.net" >> .env
echo -e "MLFLOW_S3_ENDPOINT_URL=https://storage.yandexcloud.net" >> .env
echo -e "MLFLOW_TRACKING_URI=http://127.0.0.1:5000" >> .env
echo -e "AWS_ACCESS_KEY_ID=${access_key}" >> .env
echo -e "AWS_SECRET_ACCESS_KEY=${secret_key}" >> .env
echo -e "KAFKA_BROKER=${kafka_broker}" >> .env
echo -e "INPUT_TOPIC_NAME=${topic_name}" >> .env

# AIRFLOW_UID=50000

source .env
docker compose up &

# устанавливаем minikube и helm-репозитоири для развёртывания основного приложения, а также сервсов мониторинга и airflow
curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 \
  && chmod +x minikube
  
sudo mkdir -p /usr/local/bin/
sudo install minikube /usr/local/bin/

minikube start \
    --network-plugin=cni \
    --enable-default-cni \
    --container-runtime=containerd \
    --bootstrapper=kubeadm

minikube status

sudo apt-get update && sudo apt-get install -y apt-transport-https ca-certificates gnupg
sudo mkdir -p -m 755 /etc/apt/keyrings
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.33/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
sudo chmod 644 /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.33/deb/ /' | sudo tee /etc/apt/sources.list.d/kubernetes.list
sudo chmod 644 /etc/apt/sources.list.d/kubernetes.list
sudo apt-get update
sudo apt-get install -y kubectl

curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update

helm upgrade --install fraud helm/fraud --namespace fraud --set aws.access=${access_key} --set aws.secret=${secret_key} --create-namespace 

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
kubectl create ns monitoring

helm install kps prometheus-community/kube-prometheus-stack --namespace monitoring --set grafana.adminPassword='admin' --set prometheus.prometheusSpec.scrapeInterval='15s' --create-namespace 

helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace --values helm/airflow/values.yaml

# запускаем процессинг данных из kafka
kubectl run kafka-load-test -n fraud -it --rm --image=confluentinc/cp-kafkacat:latest --command -- /bin/sh -c "
  kafkacat -b $KAFKA_BROKER -t $INPUT_TOPIC_NAME -C -o beginning -q | \
  while read line; do
    echo \"Sending: \$line\"
    curl -X POST http://fraud:80/api/predict \
      -H 'Content-Type: application/json' \
      -d \"\$line\" \
      -s -o /dev/null -w 'Status: %{http_code}\n'
    sleep 0.1
  done
"

# for debug
# kubectl -n monitoring port-forward svc/kps-grafana 3000:80
# kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 9090:9090