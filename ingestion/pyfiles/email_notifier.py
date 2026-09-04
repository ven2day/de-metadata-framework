import base64
import os
import requests
from ingestion.env.DE_Ingestion_properties import BREVO_API_KEY, BREVO_FROM_EMAIL, NOTIFY_EMAIL
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)

_BREVO_URL = "https://api.brevo.com/v3/smtp/email"


def send_pipeline_notification(
    app_name: str,
    ingest_date: str,
    status: str,
    error_msg: str | None = None,
    log_path: str | None = None,
    app_id: str | None = None,
) -> None:
    if status == "success":
        return

    if not BREVO_API_KEY:
        logger.warning("BREVO_API_KEY not set — skipping email notification")
        return
    if not NOTIFY_EMAIL:
        logger.warning("NOTIFY_EMAIL not set — skipping email notification")
        return

    succeeded = status == "success"
    app_id_str = f" [{app_id}]" if app_id else ""
    subject = f"[Pipeline] {app_name} — {'Completed' if succeeded else 'Failed'} ({ingest_date}){app_id_str}"

    if succeeded:
        body = f"""
        <div style="font-family:sans-serif;max-width:520px">
          <h2 style="color:#16a34a">&#10003; Pipeline Completed</h2>
          <p><b>Application:</b> {app_name}</p>
          <p><b>Ingest Date:</b> {ingest_date}</p>
          <p style="color:#6b7280">The pipeline finished without errors.</p>
        </div>
        """
    else:
        body = f"""
        <div style="font-family:sans-serif;max-width:520px">
          <h2 style="color:#dc2626">&#10007; Pipeline Failed</h2>
          <p><b>Application:</b> {app_name}</p>
          <p><b>Ingest Date:</b> {ingest_date}</p>
          <p><b>Error:</b></p>
          <pre style="background:#f3f4f6;padding:12px;border-radius:4px;font-size:12px">{error_msg or 'Unknown error'}</pre>
        </div>
        """

    payload = {
        "sender": {"name": "DE Pipeline", "email": BREVO_FROM_EMAIL},
        "to": [{"email": NOTIFY_EMAIL}],
        "subject": subject,
        "htmlContent": body,
    }

    if status != "success" and log_path and os.path.exists(log_path):
        try:
            with open(log_path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")
            filename = os.path.basename(log_path).replace(".log", ".txt")
            payload["attachment"] = [{"content": encoded, "name": filename}]
            logger.info("Log file attached to failure email [file=%s]", filename)
        except Exception as exc:
            logger.warning("Could not attach log file to email: %s", exc)

    logger.info("Sending email notification [status=%s, to=%s]", status, NOTIFY_EMAIL)
    try:
        resp = requests.post(
            _BREVO_URL,
            json=payload,
            headers={"api-key": BREVO_API_KEY, "Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code in (200, 201):
            logger.info("Email notification delivered [to=%s, subject=%s]", NOTIFY_EMAIL, subject)
        else:
            logger.error(
                "Email notification rejected by Brevo [status=%d, to=%s]: %s",
                resp.status_code, NOTIFY_EMAIL, resp.text,
            )
    except Exception as exc:
        logger.error("Email notification failed [to=%s]: %s", NOTIFY_EMAIL, exc)
