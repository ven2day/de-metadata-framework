"""Create the first UI root account without logging its credentials."""

import os
import sys


sys.path.insert(0, "/app")

from ui.db import get_conn, init_schema
from ui.models import User


def main() -> None:
    print("[seed] Initialising schema...")
    init_schema()

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM public.app_users WHERE role = 'root' LIMIT 1")
        exists = cur.fetchone()

    if exists:
        print("[seed] Root user already exists - skipping.")
        return

    password = os.environ.get("ROOT_PASSWORD", "").strip()
    if not password or password.startswith("REQUIRED_"):
        print("[seed] ROOT_PASSWORD is required for the initial root user.")
        print("[seed] Set it in .env; this script never prints passwords.")
        sys.exit(1)

    errors = User.validate_password(password)
    if errors:
        print("[seed] ROOT_PASSWORD does not meet requirements:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)

    root_email = os.environ.get("ROOT_EMAIL", "root@localhost")
    User.create("root", root_email, password, role="root")
    print("[seed] Root user created with the provided ROOT_PASSWORD.")
    print("[seed] Done.")


if __name__ == "__main__":
    main()
