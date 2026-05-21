if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import asyncio
from asyncio import TaskGroup, get_running_loop, to_thread
from asyncio.queues import Queue
from ftplib import FTP, _SSLSocket  # type: ignore
from io import BytesIO
from json import loads
from logging import getLogger
from pathlib import PurePosixPath
from re import Pattern, compile
from socket import gaierror
from typing import NoReturn, Self

from environment_init_vars import SETTINGS
from err_handling import handle_fatal_exc_async
from imap_tools import MailMessage

logger = getLogger(__name__)


class ServerNotAvailableError(ConnectionError):
  pass


class SFTFTPClient(FTP):
  creds = loads(SETTINGS.sft_ftp_creds_file.read_text())

  def __enter__(self) -> Self:
    try:
      self.connect(host=self.creds["HOST"], port=self.creds["PORT"])
      self.login(user=self.creds["USER"], passwd=self.creds["PWD"])
    except ConnectionRefusedError as e:
      raise ServerNotAvailableError(
        f"Could not connect to FTP server at {self.creds['HOST']}:{self.creds['PORT']}"
        f"\n Server exists but is not running an FTP service or is blocking the connection."
      ) from e
    except TimeoutError as e:
      raise ServerNotAvailableError(
        f"Connection to FTP server at {self.creds['HOST']}:{self.creds['PORT']} timed out."
        f"\n Server may be offline or experiencing connectivity issues."
      ) from e
    except gaierror as e:
      raise ServerNotAvailableError(f"FTP server hostname {self.creds['HOST']} could not be resolved.\n DNS has likely failed") from e
    return self

  def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    self.quit()


@handle_fatal_exc_async
async def direct_email_processing(queue: Queue[MailMessage]) -> NoReturn:
  """Continuously check for new emails and process them."""
  loop = get_running_loop()
  async with TaskGroup() as subtasks:
    while True:
      email_data = await queue.get()

      subtasks.create_task(to_thread(process_email, email_data=email_data, queue=queue, loop=loop))


# Regex pattern for matching email subjects
# test - Wed, Apr 8, 2026 3:15 PM
SUBJECT_PATTERN: Pattern = compile(
  r"(?P<report_name>.*) - (?P<timestamp>"
  r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun), "
  r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) "
  r"\d{1,2}, \d{4} \d{1,2}:\d{2} (AM|PM))"
)


def process_email(email_data: MailMessage, queue: Queue[MailMessage], loop: asyncio.AbstractEventLoop) -> None:
  """Process a single email message."""
  # Placeholder for actual email processing logic
  logger.info(f"Processing email with subject: {email_data.subject}")

  if match := SUBJECT_PATTERN.match(email_data.subject):
    report_name = match.group("report_name")
    # timestamp = match.group("timestamp")

    try:
      with SFTFTPClient() as ftp_client:
        # check if the a directory with a name that matches the report name exists on the FTP server, if not create it
        if report_name not in ftp_client.nlst():
          ftp_client.mkd(report_name)

        remote_paths = {PurePosixPath(report_name) / attach.filename: attach.payload for attach in email_data.attachments}

        for remote_path, payload in remote_paths.items():
          bio = BytesIO(payload)
          with ftp_client.transfercmd(f"STOR {remote_path.as_posix()}") as write_file:
            while buffer := bio.read(8192):
              write_file.sendall(buffer)
            if _SSLSocket is not None and isinstance(write_file, _SSLSocket):
              write_file.unwrap()  # type: ignore
          ftp_client.voidresp()

        logger.info(f"Successfully processed email '{email_data.subject}' and uploaded attachments to FTP server.")

    except ServerNotAvailableError as e:
      logger.error(f"Failed to process email due to FTP server issues: {e}")
      # re-queue from this worker thread back onto the asyncio loop's queue safely
      loop.call_soon_threadsafe(queue.put_nowait, email_data)
