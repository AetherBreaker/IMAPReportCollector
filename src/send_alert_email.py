if __name__ == "__main__":
  from sys import platform

  from logging_config import configure_logging
  from rich.console import Console

  RICH_CONSOLE = Console(width=None if platform == "win32" else 175, log_time=platform == "win32")

  configure_logging(RICH_CONSOLE)
else:
  from logging_config import RICH_CONSOLE

import smtplib
import ssl
from email.message import EmailMessage
from logging import getLogger

from environment_init_vars import SETTINGS

logger = getLogger(__name__)


ALERTS_EMAIL = SETTINGS.alerts_email
ALERTS_EMAIL_PWD = SETTINGS.alerts_email_pwd
ALERTS_RECIPIENTS = SETTINGS.alerts_recipients


def send_alert_email(subject: str, content: str) -> None:
  if not ALERTS_RECIPIENTS:
    logger.warning("Skipping alert email because no recipients are configured.")
    return

  msg = EmailMessage()
  msg.set_content(content)
  msg["Subject"] = subject
  msg["From"] = ALERTS_EMAIL
  msg["To"] = ", ".join([str(recipient) for recipient in ALERTS_RECIPIENTS])
  context = ssl.create_default_context()
  try:
    with smtplib.SMTP(SETTINGS.alerts_smtp_server, SETTINGS.alerts_smtp_port) as server:
      server.ehlo()
      server.starttls(context=context)
      server.ehlo()
      server.login(ALERTS_EMAIL, ALERTS_EMAIL_PWD)
      server.send_message(msg)
    logger.debug("Alert email sent successfully.")
  except Exception:
    logger.error("Failed to send alert email.", exc_info=True)
