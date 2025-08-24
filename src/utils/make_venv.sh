#!/bin/bash

python3 -m venv .temp_venv
source .temp_venv/bin/activate
.temp_venv/bin/pip install --upgrade pip
.temp_venv/bin/pip install wheel
.temp_venv/bin/pip install "otus_mlops[train] @ https://storage.yandexcloud.net/brusia-bucket/src/pypi/otus_mlops-0.1.0.tar.gz"
.temp_venv/bin/pip install venv-pack
mkdir -p venvs/
venv-pack -o venvs/venv.tar.gz
