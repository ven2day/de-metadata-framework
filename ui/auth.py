from functools import wraps

from flask import Blueprint, render_template, redirect, url_for, request, abort
from flask_login import login_user, logout_user, login_required, current_user

from ui.extensions import limiter
from ui.models import User

auth_bp = Blueprint("auth", __name__)


def root_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_root:
            abort(403)
        return f(*args, **kwargs)
    return decorated


@auth_bp.route("/login", methods=["GET", "POST"])
@limiter.limit("5 per minute")
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = User.get_by_username(username)

        if not user or not user.is_active:
            error = "Invalid credentials."
        elif user.is_locked():
            error = "Account temporarily locked. Try again in 15 minutes."
        elif not user.verify_password(password):
            User.record_login_failure(user.id)
            error = "Invalid credentials."
        else:
            User.record_login_success(user.id)
            login_user(user, remember=False)
            return redirect(request.args.get("next") or url_for("index"))

    return render_template("login.html", error=error)


@auth_bp.route("/logout", methods=["POST"])
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login"))


@auth_bp.route("/users", methods=["GET", "POST"])
@login_required
@root_required
def users():
    errors = []
    success = None

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email    = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        confirm  = request.form.get("confirm_password", "")

        if not username or not email or not password:
            errors.append("All fields are required.")
        elif password != confirm:
            errors.append("Passwords do not match.")
        elif User.get_by_username(username):
            errors.append(f"Username '{username}' is already taken.")
        else:
            pw_errors = User.validate_password(password)
            if pw_errors:
                errors.extend(pw_errors)
            else:
                try:
                    User.create(
                        username, email, password,
                        role="user", created_by=current_user.id,
                    )
                    success = f"User '{username}' created successfully."
                except Exception as exc:
                    errors.append(f"Database error: {exc}")

    return render_template(
        "users.html",
        users=User.all_users(),
        errors=errors,
        success=success,
    )
