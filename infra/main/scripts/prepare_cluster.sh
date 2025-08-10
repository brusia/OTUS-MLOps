#!/bin/bash
set -euxo pipefail
echo "Starting cluster initialization..."

# apt-get update && apt-get install python3-venv
# python3 -m venv /tmp/train-venv
# source /tmp/train-venv/bin/activate
# python3 -m pip install uv
# python3 -m uv venv /tmp/train-venv
# source /tmp/train-venv/bin/activate
# python3 -m pip install venv
python3 -m pip install "otus_mlops[train] @ https://storage.yandexcloud.net/brusia-bucket/src/pypi/otus_mlops-0.1.0.tar.gz"

# curl -O https://storage.yandexcloud.net/brusia-bucket/src/pypi/otus_mlops-0.1.0.tar.gz
# python3 -m pip install otus_mlops[train] otus_mlops-0.1.0.tar.gz