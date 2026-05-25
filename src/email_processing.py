# heartrate
if __name__ == "__main__":
  from sft_ext.logging_ext.init_logging import init_logging

  init_logging()

from asyncio import AbstractEventLoop, Queue, TaskGroup, get_running_loop, to_thread
from io import BytesIO
from logging import getLogger
from pathlib import PurePosixPath
from re import Pattern, compile

from environment_init_vars import SETTINGS
from ftp_configs import SFTSFTPClient
from imap_tools import MailMessage
from sft_ext.errors.err_handling import FATAL_EVENT, handle_fatal_exc_async
from sft_ext.ftp.adapter import AdaptedSFTP, FTPAdapter, ServerNotAvailableError

logger = getLogger(__name__)


@handle_fatal_exc_async
async def direct_email_processing(queue: Queue[MailMessage]):
  """Continuously check for new emails and process them."""
  loop = get_running_loop()
  async with TaskGroup() as subtasks:
    while True:
      if FATAL_EVENT.is_set():
        logger.error("Fatal event detected. Stopping email processing.")
        break
      logger.info("Waiting for emails to be added to queue...")
      email_data = await queue.get()
      logger.info(f"Email with subject '{email_data.subject}' retrieved from queue for processing.")
      subtasks.create_task(to_thread(process_email, email_data=email_data, queue=queue, loop=loop))


# Regex pattern for matching email subjects
# test - Wed, Apr 8, 2026 3:15 PM
SUBJECT_PATTERN: Pattern = compile(
  r"^(Report: )?(?P<report_name>.*) - (?P<timestamp>"
  r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun), "
  r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) "
  r"\d{1,2}, \d{4} \d{1,2}:\d{2} (AM|PM))$"
)

SWEETFIRE_SFTP: FTPAdapter[AdaptedSFTP] = FTPAdapter(SFTSFTPClient, container_cls="", tzinfo=SETTINGS.tz)
BASE_DIR = PurePosixPath("/upload")


def process_email(email_data: MailMessage, queue: Queue[MailMessage], loop: AbstractEventLoop) -> None:
  # sourcery skip: extract-method
  """Process a single email message."""
  # Placeholder for actual email processing logic
  logger.info(f"Processing email with subject: {email_data.subject}")

  if match := SUBJECT_PATTERN.match(email_data.subject):
    report_name = match.group("report_name")
    # timestamp = match.group("timestamp")
    logger.info(f"Email subject matched expected pattern. Extracted report name: '{report_name}'")

    logger.info("Testing FTP connection to server...")
    if not SWEETFIRE_SFTP.test_connection():
      logger.error("FTP server is not available. Re-queuing email for later processing.")
      loop.call_soon_threadsafe(queue.put_nowait, email_data)
      return

    try:
      with SWEETFIRE_SFTP.start_session() as sftp_client:
        logger.info(f"Connected to FTP server. Preparing to upload attachments for report '{report_name}'")
        target_folder = BASE_DIR / report_name

        # check if the a directory with a name that matches the report name exists on the FTP server, if not create it
        logger.info(f"Querying FTP for {target_folder}")
        dirs = [entry.filename for entry in sftp_client.listdir(path=BASE_DIR.as_posix())]
        logger.info(f"Query for {target_folder} complete")
        if str(target_folder.name) not in dirs:
          sftp_client.makedir(target_folder.as_posix())
          logger.info(f"Created new directory on FTP server: {target_folder}")

        logger.info("Directory check complete. Starting attachment upload...")

        remote_paths = {(target_folder / attach.filename): attach.payload for attach in email_data.attachments}

        for remote_path, payload in remote_paths.items():
          bio = BytesIO(payload)
          sftp_client.upload_file(
            remote_path=remote_path.as_posix(),
            callback=bio.read,
            file_size=len(payload),
          )
          logger.info(f"Attachment '{remote_path.name}' uploaded to '{remote_path.as_posix()}'")

      logger.info(f"Successfully processed email '{email_data.subject}' and uploaded attachments to FTP server.")

      loop.call_soon_threadsafe(queue.task_done)

    except ServerNotAvailableError as e:
      logger.error(f"Failed to process email due to FTP server issues: {e}")
      # re-add the email to the queue for retry after some delay
      # In a real implementation, you might want to implement an exponential backoff strategy here
      loop.call_soon_threadsafe(queue.put_nowait, email_data)

  else:
    logger.warning(f"Email subject '{email_data.subject}' did not match expected pattern. Skipping")
