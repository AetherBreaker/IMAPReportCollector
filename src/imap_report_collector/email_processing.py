# heartrate
if __name__ == "__main__":
  # First party imports
  from aeth_ext import initialize

  initialize(asyncio=True)

# Standard library imports
from asyncio import AbstractEventLoop, Queue, TaskGroup, get_running_loop, to_thread
from io import BytesIO
from json import loads
from logging import getLogger
from pathlib import PurePosixPath
from re import Pattern, compile
from typing import TYPE_CHECKING

# Third party imports
from pydantic import SecretStr

# First party imports
from aeth_ext.errors.err_handling import handle_fatal_exc_async
from aeth_ext.errors.shutdown import SHUTDOWN
from aeth_ext.ftp import create_ftp_adapter
from aeth_ext.ftp.credentials import SFTPCredentials
from aeth_ext.ftp.errors import ServerNotAvailableError

# Local folder imports
from .environment_init_vars import SETTINGS

if TYPE_CHECKING:
  # Third party imports
  from imap_tools import MailMessage

logger = getLogger(__name__)


@handle_fatal_exc_async
async def direct_email_processing(queue: Queue[MailMessage]):
  """Continuously check for new emails and process them."""
  loop = get_running_loop()
  async with TaskGroup() as subtasks:
    while True:
      if SHUTDOWN.is_set():
        logger.error("Shutdown signal detected. Stopping email processing.")
        break
      logger.info("Waiting for emails to be added to queue...")
      email_data = await queue.get()
      logger.info("Email with subject '%s' retrieved from queue for processing.", email_data.subject)
      subtasks.create_task(to_thread(process_email, email_data=email_data, queue=queue, loop=loop))


# Regex pattern for matching email subjects
# test - Wed, Apr 8, 2026 3:15 PM
SUBJECT_PATTERN: Pattern[str] = compile(
  r"^(Report: )?(?P<report_name>.*) - (?P<timestamp>"
  r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun), "
  r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) "
  r"\d{1,2}, \d{4} \d{1,2}:\d{2} (AM|PM))$"
)

# Keep the parsed plaintext only while constructing the redacting credentials object -- the
# password lives on as a `SecretStr` (rendered as `**********` by repr/str/logging) and is only
# unwrapped by aeth_ext at the paramiko connect call.
_raw = loads(SETTINGS.sft_website_creds_file.read_text())
try:
  SWEETFIRE_SFTP = create_ftp_adapter(
    SFTPCredentials(
      host=_raw["HOSTNAME"],
      username=_raw["USER"],
      password=SecretStr(_raw["PWD"]),
      port=int(_raw.get("PORT", 22)),
      host_key_policy="auto_add",
    ),
    container_cls="SweetfireSFTP",
    tzinfo=SETTINGS.tz,
  )
finally:
  del _raw

BASE_DIR = PurePosixPath("/upload")


def process_email(email_data: MailMessage, queue: Queue[MailMessage], loop: AbstractEventLoop) -> None:
  # sourcery skip: extract-method
  """Process a single email message."""
  # Placeholder for actual email processing logic
  logger.info("Processing email with subject: %s", email_data.subject)

  if match := SUBJECT_PATTERN.match(email_data.subject):
    report_name = match.group("report_name")
    # timestamp = match.group("timestamp")
    logger.info("Email subject matched expected pattern. Extracted report name: '%s'", report_name)

    logger.info("Testing FTP connection to server...")
    if not SWEETFIRE_SFTP.test_connection():
      logger.error("FTP server is not available. Re-queuing email for later processing.")
      loop.call_soon_threadsafe(queue.put_nowait, email_data)
      return

    try:
      with SWEETFIRE_SFTP.start_session() as sftp_client:
        logger.info("Connected to FTP server. Preparing to upload attachments for report '%s'", report_name)
        target_folder = BASE_DIR / report_name

        # check if the a directory with a name that matches the report name exists on the FTP server, if not create it
        logger.info("Querying FTP for %s", target_folder)
        dirs = [entry.filename for entry in sftp_client.listdir(path=BASE_DIR.as_posix())]
        logger.info("Query for %s complete", target_folder)
        if str(target_folder.name) not in dirs:
          sftp_client.makedir(target_folder.as_posix())
          logger.info("Created new directory on FTP server: %s", target_folder)

        logger.info("Directory check complete. Starting attachment upload...")

        remote_paths = {(target_folder / attach.filename): attach.payload for attach in email_data.attachments}

        for remote_path, payload in remote_paths.items():
          bio = BytesIO(payload)
          sftp_client.upload_file(
            remote_path=remote_path.as_posix(),
            callback=bio.read,
            file_size=len(payload),
          )
          logger.info("Attachment '%s' uploaded to '%s'", remote_path.name, remote_path.as_posix())

      logger.info("Successfully processed email '%s' and uploaded attachments to FTP server.", email_data.subject)

      loop.call_soon_threadsafe(queue.task_done)

    except ServerNotAvailableError as e:
      logger.error("Failed to process email due to FTP server issues: %s", e)
      # re-add the email to the queue for retry after some delay
      # In a real implementation, you might want to implement an exponential backoff strategy here
      loop.call_soon_threadsafe(queue.put_nowait, email_data)

  else:
    logger.warning("Email subject '%s' did not match expected pattern. Skipping", email_data.subject)
