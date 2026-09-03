"""Pydantic settings for the IMAP watch connection and its polling/timeout knobs."""

# Standard library imports
from logging import getLogger
from os import environ
from typing import TYPE_CHECKING, Annotated

# Third party imports
from pydantic import Field, SecretStr

# First party imports
from aeth_ext.settings import BaseSettings

if TYPE_CHECKING:
  # Standard library imports
  from pathlib import Path

logger = getLogger(__name__)

environ.setdefault("PYDANTIC_ERRORS_INCLUDE_URL", "false")


class Settings(BaseSettings):
  """Environment-backed settings (`WATCH_*` IMAP connection, polling and timeout knobs, `REALTIME_MONITOR`)."""

  watch_imap_server: Annotated[str, Field(alias="WATCH_IMAP_SERVER")] = "imappro.zoho.com"
  watch_imap_port: Annotated[int, Field(alias="WATCH_IMAP_PORT")] = 993
  watch_email: Annotated[str, Field(alias="WATCH_EMAIL")] = "info@sweetfiretobacco.com"
  watch_email_pwd: Annotated[SecretStr, Field(alias="WATCH_EMAIL_PWD")]

  watch_polling_timeout_sec: Annotated[int, Field(alias="WATCH_POLLING_TIMEOUT_SEC")] = 10

  # Kept short (well under aeth_ext's 7s GRACEFUL shutdown budget) so a stalled login/fetch
  # resolves in time for the monitoring thread to notice shutdown and exit cleanly, rather than
  # holding the shutdown sequence's thread-join hostage. A single short timeout is tolerated
  # without complaint -- see watch_max_consecutive_timeouts -- so this isn't a false-positive risk
  # on an otherwise healthy but momentarily slow connection.
  watch_socket_timeout_sec: Annotated[float, Field(alias="WATCH_SOCKET_TIMEOUT_SEC")] = 5
  watch_max_consecutive_timeouts: Annotated[int, Field(alias="WATCH_MAX_CONSECUTIVE_TIMEOUTS")] = 5

  realtime_monitor: Annotated[bool, Field(alias="REALTIME_MONITOR")] = False

  @property
  def sft_website_creds_file(self) -> Path:
    """Path to the SFT SFTP credentials JSON under the secrets folder."""
    return self._creds_file_reusable("SFT website creds file not found at expected location", "secrets", "sft_ftp_creds.json")
