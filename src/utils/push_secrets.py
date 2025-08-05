"""
Script: add_secrets_to_github_repo.py
Description: This script adds secrets to a GitHub repository using the GitHub API.
"""

import base64
import logging
import requests
from dotenv import dotenv_values
from nacl import public

logger = logging.getLogger(__name__)
secrets = dotenv_values(".env")
secrets_blacklist = ["GITHUB_TOKEN", "GITHUB_REPO", "PRIVATE_KEY_PATH"]

GITHUB_TOKEN = secrets.get("GITHUB_TOKEN")
GITHUB_REPO = secrets.get("GITHUB_REPO")

if not GITHUB_TOKEN or not GITHUB_REPO:
    raise ValueError("GITHUB_TOKEN и GITHUB_REPO должны быть определены в .env")

API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/actions/secrets"

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

def encrypt_secret(public_key: str, secret_value: str) -> str:
    """
    Encrypt a secret value using the repository's public key.
    """
    public_key_bytes = base64.b64decode(public_key)
    public_key_obj = public.PublicKey(public_key_bytes)
    sealed_box = public.SealedBox(public_key_obj)
    encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
    return base64.b64encode(encrypted).decode("utf-8")


def add_secret(name: str, value: str) -> None:
    """
    Add secret to GitHub repository
    """
    response = requests.get(f"{API_URL}/public-key", headers=HEADERS, timeout=10)
    response.raise_for_status()
    public_key_data = response.json()

    encrypted_value = encrypt_secret(public_key_data["key"], value)

    # Создание секрета
    data = {
        "encrypted_value": encrypted_value,
        "key_id": public_key_data["key_id"]
    }
    response = requests.put(f"{API_URL}/{name}", headers=HEADERS, json=data, timeout=10)
    if response.status_code == 201:
        logger.info(f"Secret {name} created successfully.")
    elif response.status_code == 204:
        logger.info(f"Secret {name} updated successfully.")
    else:
        logger.error(f"Failed to add secret {name}. Response: {response.text}")

def main() -> None:
    """
    Main function to add secrets to GitHub repository
    """
    logger.info("Starting to add secrets to GitHub repository")

    for key, value in secrets.items():
        if key in secrets_blacklist:
            logger.debug(f"Skipping blacklisted secret: {key}")
            continue

        if not value.strip():
            logger.warning(f"Skipping secret {key}: value is empty")
            continue

        logger.info(f"Processing secret: {key}")
        add_secret(key, value)

    logger.info("Finished processing all secrets")

if __name__ == "__main__":
    main()