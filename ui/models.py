import hashlib
import re
from datetime import datetime, timezone

import requests as _req
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, VerificationError, InvalidHashError
from flask_login import UserMixin

from ui.db import cursor, get_conn

_ph = PasswordHasher(
    time_cost=2,
    memory_cost=65536,
    parallelism=2,
    hash_len=32,
    salt_len=16,
)

_UPPER   = re.compile(r"[A-Z]")
_LOWER   = re.compile(r"[a-z]")
_DIGIT   = re.compile(r"\d")
_SPECIAL = re.compile(r"[!@#$%^&*()\-_=+\[\]{};:'\",.<>?/\\|`~]")


class User(UserMixin):
    def __init__(self, row: dict):
        self.id              = row["id"]
        self.username        = row["username"]
        self.email           = row["email"]
        self.password_hash   = row["password_hash"]
        self.role            = row["role"]
        self._is_active      = row["is_active"]
        self.created_at      = row["created_at"]
        self.last_login      = row["last_login"]
        self.failed_attempts = row["failed_attempts"]
        self.locked_until    = row.get("locked_until")

    def get_id(self) -> str:
        return str(self.id)

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def is_root(self) -> bool:
        return self.role == "root"

    def is_locked(self) -> bool:
        if not self.locked_until:
            return False
        lu = self.locked_until
        if lu.tzinfo is None:
            lu = lu.replace(tzinfo=timezone.utc)
        return lu > datetime.now(timezone.utc)

    def verify_password(self, password: str) -> bool:
        try:
            return _ph.verify(self.password_hash, password)
        except (VerifyMismatchError, VerificationError, InvalidHashError):
            return False

    @staticmethod
    def hash_password(password: str) -> str:
        return _ph.hash(password)

    @staticmethod
    def get_by_id(user_id: int) -> "User | None":
        with cursor() as cur:
            cur.execute("SELECT * FROM public.app_users WHERE id = %s", (user_id,))
            row = cur.fetchone()
        return User(row) if row else None

    @staticmethod
    def get_by_username(username: str) -> "User | None":
        with cursor() as cur:
            cur.execute(
                "SELECT * FROM public.app_users WHERE username = %s", (username,)
            )
            row = cur.fetchone()
        return User(row) if row else None

    @staticmethod
    def all_users() -> "list[User]":
        with cursor() as cur:
            cur.execute("SELECT * FROM public.app_users ORDER BY created_at")
            rows = cur.fetchall()
        return [User(r) for r in rows]

    @staticmethod
    def create(
        username: str,
        email: str,
        password: str,
        role: str = "user",
        created_by: "int | None" = None,
    ) -> "User":
        ph = User.hash_password(password)
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO public.app_users
                       (username, email, password_hash, role, created_by)
                   VALUES (%s, %s, %s, %s, %s)
                   RETURNING id""",
                (username, email, ph, role, created_by),
            )
            new_id = cur.fetchone()[0]
        conn.commit()
        return User.get_by_id(new_id)

    @staticmethod
    def record_login_success(user_id: int):
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE public.app_users
                   SET last_login = NOW(), failed_attempts = 0, locked_until = NULL
                   WHERE id = %s""",
                (user_id,),
            )
        conn.commit()

    @staticmethod
    def record_login_failure(user_id: int):
        """Increment failed attempts; lock account for 15 min after 5 consecutive failures."""
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE public.app_users
                   SET failed_attempts = failed_attempts + 1,
                       locked_until = CASE
                           WHEN failed_attempts + 1 >= 5
                           THEN NOW() + INTERVAL '15 minutes'
                           ELSE locked_until
                       END
                   WHERE id = %s""",
                (user_id,),
            )
        conn.commit()

    @staticmethod
    def validate_password(password: str) -> "list[str]":
        errors = []
        if len(password) < 12:
            errors.append("At least 12 characters required.")
        if not _UPPER.search(password):
            errors.append("At least one uppercase letter required.")
        if not _LOWER.search(password):
            errors.append("At least one lowercase letter required.")
        if not _DIGIT.search(password):
            errors.append("At least one digit required.")
        if not _SPECIAL.search(password):
            errors.append("At least one special character required.")
        if _is_pwned(password):
            errors.append(
                "This password appeared in a known data breach — choose a different one."
            )
        return errors


def _is_pwned(password: str) -> bool:
    """k-anonymity check against HaveIBeenPwned. Fails open on network error."""
    try:
        sha1 = hashlib.sha1(password.encode()).hexdigest().upper()
        prefix, suffix = sha1[:5], sha1[5:]
        r = _req.get(
            f"https://api.pwnedpasswords.com/range/{prefix}", timeout=3
        )
        for line in r.text.splitlines():
            h, _ = line.split(":")
            if h == suffix:
                return True
    except Exception:
        pass
    return False
