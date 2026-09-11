from flask_wtf.csrf import CSRFProtect
from flask_login import LoginManager
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask import request as _request


def _real_ip():
    """Return client IP, trusting X-Forwarded-For when behind Cloudflare."""
    import os
    if os.environ.get("BEHIND_HTTPS_PROXY", "0") == "1":
        forwarded = _request.headers.get("X-Forwarded-For", "")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return get_remote_address()


csrf          = CSRFProtect()
login_manager = LoginManager()
limiter       = Limiter(_real_ip)

login_manager.login_view    = "auth.login"
login_manager.login_message = ""
