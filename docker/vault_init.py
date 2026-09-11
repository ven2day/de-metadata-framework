#!/usr/bin/env python3
"""
Vault bootstrap + seal-monitor daemon.

On start:
  1. Wait for Vault API to respond.
  2. Initialize Vault if needed (1 key share / threshold 1).
  3. Unseal if sealed.
  4. Mount engines, seed secrets, create scoped pipeline token.

After bootstrap, loops every 30 s watching for seal events (e.g. after a
container restart) and re-unseals + refreshes the pipeline token automatically.
"""
import base64
import json
import os
import sys
import time

import requests

VAULT_ADDR  = os.environ.get("VAULT_ADDR", "http://vault:8200")
VAULT_PATH  = os.environ.get("VAULT_PATH", "encryption/pii")
SALT_2      = os.environ.get("SALT_2", "")
SUPABASE_PW = os.environ.get("SUPABASE_DB_PASSWORD_PLAIN", "")

ORACLE_USER            = os.environ.get("ORACLE_USER", "")
ORACLE_PASSWORD        = os.environ.get("ORACLE_PASSWORD", "")
ORACLE_DSN             = os.environ.get("ORACLE_DSN", "")
ORACLE_WALLET_PASSWORD = os.environ.get("ORACLE_WALLET_PASSWORD", "")
ORACLE_WALLET_ZIP_B64  = os.environ.get("ORACLE_WALLET_ZIP_B64", "")

INIT_FILE        = "/vault/data/init.json"
TOKEN_FILE       = "/vault/secrets/pipeline_token"
SUPABASE_PW_FILE = "/vault/secrets/supabase_db_password_ciphertext"


# ── Vault HTTP helpers ────────────────────────────────────────────────────────

def _headers(token=None):
    return {"X-Vault-Token": token} if token else {}


def _get(path, token=None):
    return requests.get(f"{VAULT_ADDR}/v1/{path}", headers=_headers(token), timeout=5)


def _post(path, data, token=None):
    return requests.post(f"{VAULT_ADDR}/v1/{path}", json=data, headers=_headers(token), timeout=5)


def _put(path, data, token=None):
    return requests.put(f"{VAULT_ADDR}/v1/{path}", json=data, headers=_headers(token), timeout=5)


# ── Status helpers ────────────────────────────────────────────────────────────

def wait_for_vault():
    print("[vault-init] Waiting for Vault API...")
    for _ in range(60):
        try:
            r = requests.get(f"{VAULT_ADDR}/v1/sys/health", timeout=3)
            # 200=ok, 429=standby, 501=not-init, 503=sealed — all mean "up"
            if r.status_code in (200, 429, 501, 503):
                print(f"[vault-init] Vault responded (HTTP {r.status_code})")
                return
        except Exception:
            pass
        time.sleep(2)
    print("[vault-init] ERROR: Vault did not become reachable in 120 s", file=sys.stderr)
    sys.exit(1)


def vault_status():
    """Return HTTP status code from /v1/sys/health (200=unsealed, 503=sealed, 501=not-init)."""
    r = requests.get(f"{VAULT_ADDR}/v1/sys/health", timeout=5)
    return r.status_code


# ── One-time bootstrap helpers ────────────────────────────────────────────────

def enable_engine(path, engine_type, options=None, token=None):
    r = _get("sys/mounts", token)
    if f"{path}/" in r.json():
        print(f"[vault-init] {engine_type} already mounted at {path}/")
        return
    body = {"type": engine_type}
    if options:
        body["options"] = options
    r = _post(f"sys/mounts/{path}", body, token)
    r.raise_for_status()
    print(f"[vault-init] Mounted {engine_type} at {path}/")


def create_transit_key(name, exportable=False, token=None):
    r = _get(f"transit/keys/{name}", token)
    if r.status_code == 200:
        print(f"[vault-init] Transit key '{name}' already exists")
        return
    r = _post(f"transit/keys/{name}", {"type": "aes256-gcm96", "exportable": exportable}, token)
    r.raise_for_status()
    print(f"[vault-init] Created transit key '{name}' (exportable={exportable})")


def _write_pipeline_token(root_token):
    """Create a scoped pipeline token and write it to TOKEN_FILE."""
    r = _post(
        "auth/token/create",
        {"policies": ["pipeline-policy"], "ttl": "768h", "period": "768h", "renewable": True},
        root_token,
    )
    r.raise_for_status()
    pipeline_token = r.json()["auth"]["client_token"]
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    with open(TOKEN_FILE, "w") as f:
        f.write(pipeline_token)
    print(f"[vault-init] Pipeline token written → {TOKEN_FILE}")


# ── Full bootstrap (idempotent) ───────────────────────────────────────────────

def bootstrap():
    wait_for_vault()
    status = vault_status()

    # ── Initialize ────────────────────────────────────────────────────────────
    if status == 501:
        print("[vault-init] Initializing Vault (1 key share / threshold 1)...")
        r = _put("sys/init", {"secret_shares": 1, "secret_threshold": 1})
        if not r.ok:
            print(f"[vault-init] Vault initialization failed: HTTP {r.status_code}\n{r.text}", file=sys.stderr)
            r.raise_for_status()
        init_data = r.json()
        os.makedirs(os.path.dirname(INIT_FILE), exist_ok=True)
        with open(INIT_FILE, "w") as f:
            json.dump(init_data, f, indent=2)
        print(f"[vault-init] Vault initialized — credentials saved to {INIT_FILE}")
        status = 503  # now sealed

    # ── Load init data ────────────────────────────────────────────────────────
    if not os.path.exists(INIT_FILE):
        print(f"[vault-init] ERROR: {INIT_FILE} not found — cannot unseal", file=sys.stderr)
        sys.exit(1)

    with open(INIT_FILE) as f:
        init_data = json.load(f)

    root_token = init_data["root_token"]
    unseal_key = init_data["keys_base64"][0]

    # ── Unseal ────────────────────────────────────────────────────────────────
    if status == 503:
        print("[vault-init] Unsealing Vault...")
        r = _put("sys/unseal", {"key": unseal_key})
        r.raise_for_status()
        print("[vault-init] Vault unsealed")

    # ── Secrets engines ───────────────────────────────────────────────────────
    enable_engine("transit", "transit", token=root_token)
    enable_engine("secret", "kv", options={"version": "2"}, token=root_token)

    create_transit_key("pii-encrypt", exportable=True, token=root_token)
    create_transit_key("supabase-pwd", exportable=False, token=root_token)

    # ── KV secret: PII salts ──────────────────────────────────────────────────
    if not SALT_2:
        print("[vault-init] WARNING: SALT_2 env var is not set — writing empty value")
    r = _post(f"secret/data/{VAULT_PATH}", {"data": {"salt_2": SALT_2}}, root_token)
    r.raise_for_status()
    print(f"[vault-init] Wrote salt_2 → secret/data/{VAULT_PATH}")

    # ── Encrypt Supabase DB password ──────────────────────────────────────────
    os.makedirs("/vault/secrets", exist_ok=True)
    if SUPABASE_PW:
        plaintext_b64 = base64.b64encode(SUPABASE_PW.encode()).decode()
        r = _post("transit/encrypt/supabase-pwd", {"plaintext": plaintext_b64}, root_token)
        r.raise_for_status()
        ciphertext_b64 = base64.b64encode(r.json()["data"]["ciphertext"].encode()).decode()
        with open(SUPABASE_PW_FILE, "w") as f:
            f.write(ciphertext_b64)
        print("[vault-init] Supabase password encrypted → secrets volume")
    else:
        print("[vault-init] WARNING: SUPABASE_DB_PASSWORD_PLAIN not set — skipping")

    # ── Oracle ADW credentials ────────────────────────────────────────────────
    oracle_data = {
        "oracle_user":            ORACLE_USER,
        "oracle_password":        ORACLE_PASSWORD,
        "oracle_dsn":             ORACLE_DSN,
        "oracle_wallet_password": ORACLE_WALLET_PASSWORD,
        "oracle_wallet_zip_b64":  ORACLE_WALLET_ZIP_B64,
    }
    r = _post("secret/data/oracle/adw", {"data": oracle_data}, root_token)
    r.raise_for_status()
    print("[vault-init] Wrote Oracle ADW credentials → secret/data/oracle/adw")

    # ── Pipeline policy + token ───────────────────────────────────────────────
    policy_hcl = (
        'path "secret/data/*" { capabilities = ["read"] }\n'
        'path "transit/export/encryption-key/pii-encrypt" { capabilities = ["read"] }\n'
        'path "transit/decrypt/supabase-pwd" { capabilities = ["update"] }\n'
    )
    r = _put("sys/policies/acl/pipeline-policy", {"policy": policy_hcl}, root_token)
    r.raise_for_status()
    print("[vault-init] Pipeline policy written")

    _write_pipeline_token(root_token)
    print("[vault-init] Bootstrap complete.")
    return init_data


# ── Seal monitor ──────────────────────────────────────────────────────────────

def monitor(init_data):
    """Loop forever; re-unseal and refresh the pipeline token whenever Vault is sealed."""
    root_token = init_data["root_token"]
    unseal_key = init_data["keys_base64"][0]

    print("[vault-init] Seal monitor started (interval: 30 s)")
    while True:
        time.sleep(30)
        try:
            status = vault_status()
            if status == 503:
                print("[vault-init] Vault sealed — re-unsealing...")
                r = _put("sys/unseal", {"key": unseal_key})
                if r.ok:
                    print("[vault-init] Vault re-unsealed — refreshing pipeline token")
                    _write_pipeline_token(root_token)
                else:
                    print(f"[vault-init] Re-unseal failed (HTTP {r.status_code}): {r.text}", file=sys.stderr)
        except Exception as exc:
            print(f"[vault-init] Monitor error: {exc}", file=sys.stderr)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        init_data = bootstrap()
    except SystemExit:
        raise
    except Exception as exc:
        print(f"[vault-init] Bootstrap failed: {exc}", file=sys.stderr)
        sys.exit(1)

    monitor(init_data)
