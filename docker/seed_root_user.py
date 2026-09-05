"""
Seed the root user into public.app_users.

Runs once at container startup (called from ui-entrypoint.sh).
- Creates the app_users table if it does not exist.
- Does nothing if a root user already exists.
- Reads ROOT_PASSWORD from the environment; generates a secure one if not set.
- Prints the auto-generated password to stdout (visible in `docker logs de-ui`).
"""
import os
import re
import secrets
import string
import sys

sys.path.insert(0, "/app")

from ui.db import init_schema, get_conn
from ui.models import User


def _generate_password(length: int = 16) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%&*"
    while True:
        pw = "".join(secrets.choice(alphabet) for _ in range(length))
        if (
            any(c.isupper() for c in pw)
            and any(c.islower() for c in pw)
            and any(c.isdigit() for c in pw)
            and any(c in "!@#$%&*" for c in pw)
        ):
            return pw


def main():
    print("[seed] Initialising schema...")
    init_schema()

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM public.app_users WHERE role = 'root' LIMIT 1")
        exists = cur.fetchone()

    if exists:
        print("[seed] Root user already exists — skipping.")
        return

    password = os.environ.get("ROOT_PASSWORD", "").strip()
    generated = False
    if not password:
        password = _generate_password()
        generated = True

    errors = User.validate_password(password)
    if errors:
        print("[seed] ROOT_PASSWORD does not meet requirements:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

    root_email = os.environ.get("ROOT_EMAIL", "root@atestingdomain.info")
    User.create("root", root_email, password, role="root")

    if generated:
        print("[seed] ╔══════════════════════════════════════════════════╗")
        print("[seed] ║  ROOT PASSWORD (auto-generated — save this now)  ║")
        print(f"[seed] ║  {password:<48}  ║")
        print("[seed] ╚══════════════════════════════════════════════════╝")
        print("[seed] Remove ROOT_PASSWORD from .env after first login.")
    else:
        print("[seed] Root user created with the provided ROOT_PASSWORD.")

    print("[seed] Done.")


if __name__ == "__main__":
    main()
