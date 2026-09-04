#!/usr/bin/env python3
"""
One-shot Vault bootstrap: initialize, unseal, seed secrets engines.
Runs in the vault-init container on every `docker-compose up`.
Idempotent — safe to re-run against an already-configured Vault.
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

INIT_FILE   = "/vault/data/init.json"
TOKEN_FILE  = "/vault/secrets/pipeline_token"
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


# ── Bootstrap helpers ─────────────────────────────────────────────────────────

def wait_for_vault():
    print("[vault-init] Waiting for Vault API...")
    for attempt in range(60):
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
    r = requests.get(f"{VAULT_ADDR}/v1/sys/health", timeout=5)
    return r.status_code   # 200=unsealed, 501=not-init, 503=sealed


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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    wait_for_vault()

    status = vault_status()

    # ── Initialize ────────────────────────────────────────────────────────────
    if status == 501:
        print("[vault-init] Initializing Vault (1 key share / threshold 1)...")
        r = _put(
            "sys/init",
            {
                "secret_shares": 1,
                "secret_threshold": 1
            }
        )

        if not r.ok:
            print(
                f"[vault-init] Vault initialization failed: "
                f"HTTP {r.status_code}",
                file=sys.stderr
            )
            print(
                f"[vault-init] Vault response: {r.text}",
                file=sys.stderr
            )
            r.raise_for_status()

        init_data = r.json()
        os.makedirs(os.path.dirname(INIT_FILE), exist_ok=True)
        with open(INIT_FILE, "w") as f:
            json.dump(init_data, f, indent=2)
        print(f"[vault-init] Vault initialized — credentials saved to {INIT_FILE}")
        status = 503  # now sealed

    # ── Load saved init data ──────────────────────────────────────────────────
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

    # pii-encrypt: exportable — key material exported to Spark for AES encryption
    create_transit_key("pii-encrypt", exportable=True, token=root_token)
    # supabase-pwd: non-exportable — used only for transit decrypt of DB password
    create_transit_key("supabase-pwd", exportable=False, token=root_token)

    # ── KV secret: PII salts ──────────────────────────────────────────────────
    if not SALT_2:
        print("[vault-init] WARNING: SALT_2 env var is not set — writing empty value")
    r = _post(f"secret/data/{VAULT_PATH}", {"data": {"salt_2": SALT_2}}, root_token)
    r.raise_for_status()
    print(f"[vault-init] Wrote salt_2 → secret/data/{VAULT_PATH}")

    # ── Encrypt Supabase DB password via Transit ──────────────────────────────
    os.makedirs("/vault/secrets", exist_ok=True)
    if SUPABASE_PW:
        plaintext_b64 = base64.b64encode(SUPABASE_PW.encode()).decode()
        r = _post("transit/encrypt/supabase-pwd", {"plaintext": plaintext_b64}, root_token)
        r.raise_for_status()
        ciphertext     = r.json()["data"]["ciphertext"]          # vault:v1:...
        ciphertext_b64 = base64.b64encode(ciphertext.encode()).decode()
        with open(SUPABASE_PW_FILE, "w") as f:
            f.write(ciphertext_b64)
        print("[vault-init] Supabase password encrypted → written to secrets volume")
    else:
        print("[vault-init] WARNING: SUPABASE_DB_PASSWORD_PLAIN not set — skipping password encryption")

    # ── Pipeline policy ───────────────────────────────────────────────────────
    policy_hcl = (
        'path "secret/data/*" { capabilities = ["read"] }\n'
        'path "transit/export/encryption-key/pii-encrypt" { capabilities = ["read"] }\n'
        'path "transit/decrypt/supabase-pwd" { capabilities = ["update"] }\n'
    )
    r = _put("sys/policies/acl/pipeline-policy", {"policy": policy_hcl}, root_token)
    r.raise_for_status()
    print("[vault-init] Pipeline policy written")

    # ── Scoped pipeline token (renewable, 24 h period) ────────────────────────
    r = _post(
        "auth/token/create",
        {"policies": ["pipeline-policy"], "ttl": "24h", "period": "24h", "renewable": True},
        root_token,
    )
    r.raise_for_status()
    pipeline_token = r.json()["auth"]["client_token"]
    with open(TOKEN_FILE, "w") as f:
        f.write(pipeline_token)
    print(f"[vault-init] Pipeline token → {TOKEN_FILE}")

    print("[vault-init] Bootstrap complete.")


if __name__ == "__main__":
    main()
