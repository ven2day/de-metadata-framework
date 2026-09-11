import base64
import os

import hvac
from ingestion.env.DE_Ingestion_properties import VAULT_ADDR, VAULT_NAMESPACE
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)

_client: hvac.Client | None = None

_TOKEN_FILE = "/vault/secrets/pipeline_token"


def _read_token() -> str:
    """Return VAULT_TOKEN from env first, then the secrets-volume file."""
    token = os.environ.get("VAULT_TOKEN", "")
    if not token and os.path.isfile(_TOKEN_FILE):
        with open(_TOKEN_FILE) as fh:
            token = fh.read().strip()
    return token


def get_vault_client() -> hvac.Client:
    """Return an authenticated Vault client (module-level singleton with auto-renewal)."""
    global _client

    if _client is not None:
        try:
            if _client.is_authenticated():
                # Renew proactively so the 24h periodic token never expires mid-run.
                try:
                    _client.auth.token.renew_self()
                except Exception:
                    pass
                return _client
        except Exception:
            pass

    # (Re)build client — re-read token file in case vault-init wrote a fresh one.
    token = _read_token()
    logger.info("Connecting to Vault at %s", VAULT_ADDR)
    _client = hvac.Client(
        url=VAULT_ADDR,
        token=token,
        namespace=VAULT_NAMESPACE or None,
    )

    if not _client.is_authenticated():
        raise RuntimeError(
            "Vault authentication failed — verify VAULT_ADDR and VAULT_TOKEN in .env"
        )

    logger.info("Vault client authenticated")
    return _client


def get_kv_secret(
    path: str,
    key: str,
    mount_point: str = "secret",
    kv_version: int = 2,
) -> str:
    """
    Read a single key from a KV secret.

    Args:
        path:        Secret path inside the mount (e.g. 'pipeline/credentials').
        key:         Field name to retrieve from the secret data.
        mount_point: KV mount point (default: 'secret').
        kv_version:  KV engine version — 1 or 2 (default: 2).

    Returns:
        The secret value as a string.
    """
    client = get_vault_client()
    logger.info("Reading KV secret")

    if kv_version == 2:
        response = client.secrets.kv.v2.read_secret_version(
            path=path,
            mount_point=mount_point,
            raise_on_deleted_version=True,
        )
        data: dict = response["data"]["data"]
    else:
        response = client.secrets.kv.v1.read_secret(
            path=path,
            mount_point=mount_point,
        )
        data = response["data"]

    if key not in data:
        raise KeyError(f"Key '{key}' not found in Vault secret at '{mount_point}/{path}'")

    logger.info("KV secret retrieved")
    return str(data[key])


def get_transit_encryption_key(
    key_name: str = "pii-encrypt",
    key_type: str = "encryption-key",
    mount_point: str = "transit",
) -> str:
    """
    Export the latest version of a Transit encryption key.

    The key must have been created with exportable=true in Vault.

    Args:
        key_name:    Name of the Transit key (e.g. 'pipeline-key').
        key_type:    Export type — 'encryption-key', 'signing-key', or 'hmac-key'
                     (default: 'encryption-key').
        mount_point: Transit mount point (default: 'transit').

    Returns:
        Base64-encoded key material of the latest key version.
    """
    client = get_vault_client()
    logger.info("Exporting Transit key")

    response = client.secrets.transit.export_key(
        name=key_name,
        key_type=key_type,
        mount_point=mount_point,
    )

    keys: dict = response["data"]["keys"]
    latest_version = str(max(int(v) for v in keys.keys()))
    key_material: str = keys[latest_version]

    logger.info("Transit key exported")
    return key_material


def get_encrypt_value(decrypt_value, key_name, key_type="", mount_path="transit"):
    decode_key = base64.b64decode(decrypt_value).decode('utf-8')
    client = get_vault_client()
    response = base64.b64decode(
        client.secrets.transit.decrypt_data(
            name=key_name,
            ciphertext=decode_key,
            mount_point=mount_path,
        )['data']['plaintext']
    ).decode('utf-8')
    return response
