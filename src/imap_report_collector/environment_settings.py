# Standard library imports
import sys
from logging import getLogger
from os import environ
from pathlib import Path
from typing import Annotated

# Third party imports
from aeth_ext.settings import BaseSettings
from pydantic import Field
from pydantic_settings import SettingsConfigDict

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

  watch_imap_server: Annotated[str, Field(alias="WATCH_IMAP_SERVER")] = "imappro.zoho.com"
  watch_imap_port: Annotated[int, Field(alias="WATCH_IMAP_PORT")] = 993
  watch_email: Annotated[str, Field(alias="WATCH_EMAIL")] = "info@sweetfiretobacco.com"
  watch_email_pwd: Annotated[str, Field(alias="WATCH_EMAIL_PWD")]

  watch_polling_timeout_sec: Annotated[int, Field(alias="WATCH_POLLING_TIMEOUT_SEC")] = 10

  realtime_monitor: Annotated[bool, Field(alias="REALTIME_MONITOR")] = False

  @property
  def sft_website_creds_file(self) -> Path:
    return self._creds_file_reusable("SFT website creds file not found at expected location", "secrets", "sft_ftp_creds.json")
