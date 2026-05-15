if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from asyncio import TaskGroup, to_thread
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
  BASE_DIR = PurePosixPath("/FTX Scheduled Reports")

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


# @handle_fatal_exc_async
async def direct_email_processing(queue: Queue[MailMessage]) -> NoReturn:
  """Continuously check for new emails and process them."""
  async with TaskGroup() as subtasks:
    while True:
      logger.info("Waiting for emails to be added to queue...")
      email_data = await queue.get()
      logger.info(f"Email with subject '{email_data.subject}' retrieved from queue for processing.")
      subtasks.create_task(to_thread(process_email, email_data=email_data, queue=queue))


# Regex pattern for matching email subjects
# test - Wed, Apr 8, 2026 3:15 PM
SUBJECT_PATTERN: Pattern = compile(
  r"^(Report: )?(?P<report_name>.*) - (?P<timestamp>"
  r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun), "
  r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) "
  r"\d{1,2}, \d{4} \d{1,2}:\d{2} (AM|PM))$"
)


def process_email(email_data: MailMessage, queue: Queue[MailMessage]) -> None:
  # sourcery skip: extract-method
  """Process a single email message."""
  # Placeholder for actual email processing logic
  logger.info(f"Processing email with subject: {email_data.subject}")

  if match := SUBJECT_PATTERN.match(email_data.subject):
    report_name = match.group("report_name")
    # timestamp = match.group("timestamp")
    logger.info(f"Email subject matched expected pattern. Extracted report name: '{report_name}'")

    try:
      with SFTFTPClient() as ftp_client:
        logger.info(f"Connected to FTP server. Preparing to upload attachments for report '{report_name}'")
        target_folder = ftp_client.BASE_DIR / report_name

        # check if the a directory with a name that matches the report name exists on the FTP server, if not create it
        logger.info(f"Querying FTP for {target_folder}")
        dirs = [entry for entry in ftp_client.mlsd(path=ftp_client.BASE_DIR.as_posix())]
        logger.info(f"Query for {target_folder} complete")
        if str(target_folder) not in dirs:
          ftp_client.mkd(target_folder.as_posix())
          logger.info(f"Created new directory on FTP server: {target_folder}")

        logger.info("Directory check complete. Starting attachment upload...")

        remote_paths = {(target_folder / attach.filename): attach.payload for attach in email_data.attachments}

        for remote_path, payload in remote_paths.items():
          bio = BytesIO(payload)
          with ftp_client.transfercmd(f"STOR {remote_path.as_posix()}") as conn:
            logger.info(f"Transfer initiated for attachment '{remote_path.name}' to '{remote_path.as_posix()}'")
            while buffer := bio.read(8192):
              conn.sendall(buffer)
            if _SSLSocket is not None and isinstance(conn, _SSLSocket):
              conn.unwrap()  # type: ignore
          logger.info(f"Attachment '{remote_path.name}' uploaded successfully to '{remote_path.as_posix()}'")
          ftp_client.voidresp()

        logger.info(f"Successfully processed email '{email_data.subject}' and uploaded attachments to FTP server.")

    except ServerNotAvailableError as e:
      logger.error(f"Failed to process email due to FTP server issues: {e}")
      # re-add the email to the queue for retry after some delay
      # In a real implementation, you might want to implement an exponential backoff strategy here
      queue.put_nowait(email_data)

  else:
    logger.warning(f"Email subject '{email_data.subject}' did not match expected pattern. Skipping")
