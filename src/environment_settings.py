import sys
from logging import getLogger
from os import environ
from pathlib import Path
from typing import Annotated

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = getLogger(__name__)

environ.setdefault("PYDANTIC_ERRORS_INCLUDE_URL", "false")


CWD = Path(__file__).parent if getattr(sys, "frozen", False) else Path.cwd()


class Settings(BaseSettings):
  model_config = (
    SettingsConfigDict(
      env_file=CWD / ".env",
      env_file_encoding="utf-8",
      env_ignore_empty=True,
      extra="ignore",
    )
    if __debug__
    else SettingsConfigDict()
  )

  persisted_dir_loc: Annotated[Path, Field(alias="PERSISTED_DIR_LOC")] = (
    CWD / "persisted_data" if __debug__ else Path("/app/persisted_data")
  )

  alerts_smtp_server: Annotated[str, Field(alias="ALERTS_SMTP_SERVER")] = "smtppro.zoho.com"
  alerts_smtp_port: Annotated[int, Field(alias="ALERTS_SMTP_PORT")] = 587
  alerts_email: Annotated[str, Field(alias="ALERTS_EMAIL")] = "info@sweetfiretobacco.com"
  alerts_email_pwd: Annotated[str, Field(alias="ALERTS_EMAIL_PWD")]
  alerts_recipients: Annotated[set[str], Field(alias="ALERTS_RECIPIENTS")] = set()

  watch_imap_server: Annotated[str, Field(alias="WATCH_IMAP_SERVER")] = "imappro.zoho.com"
  watch_imap_port: Annotated[int, Field(alias="WATCH_IMAP_PORT")] = 993
  watch_email: Annotated[str, Field(alias="WATCH_EMAIL")] = "info@sweetfiretobacco.com"
  watch_email_pwd: Annotated[str, Field(alias="WATCH_EMAIL_PWD")]

  watch_polling_timeout_sec: Annotated[int, Field(alias="WATCH_POLLING_TIMEOUT_SEC")] = 60

  @property
  def sft_ftp_creds_file(self) -> Path:
    if __debug__:
      fp = CWD / "secrets" / "sft_ftp_creds.json"
    else:
      fp = self.persisted_dir_loc / "secrets" / "sft_ftp_creds.json"
    if not fp.exists() or not fp.is_file():
      raise FileNotFoundError(f"SFTP/FTP credentials file not found at expected location: {fp}")
    return fp
