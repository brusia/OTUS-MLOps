FROM python:3.12-slim-bookworm

ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

# hadolint ignore=DL3013
RUN python3 -m pip install uv --no-cache-dir

WORKDIR /app
COPY ./src/otus_mlops/fast_api/ /app/
COPY ./pyproject.toml /app/pyproject.toml
COPY ./uv.lock ./uv.lock

RUN python3 -m uv sync --extra fast-api

CMD ["bash", "/app/fast_api.sh"]