# heartrate
if __name__ == "__main__":
  # First party imports
  from sft_ext.logging.init_logging import init_logging

  init_logging()

# Standard library imports
from datetime import date
from imaplib import IMAP4
from logging import getLogger
from re import compile
from ssl import create_default_context, socket_error
from time import sleep
from typing import TYPE_CHECKING

# Third party imports
from imap_tools import A, MailBox, MailMessage

# First party imports
from environment_init_vars import SETTINGS
from sft_ext.errors.err_handling import FATAL_EVENT, handle_fatal_exc_sync

if TYPE_CHECKING:
  # Standard library imports
  from asyncio import AbstractEventLoop
  from asyncio.queues import Queue

# from imap_tools import UidRange

logger = getLogger(__name__)


STATIC_DATE_FILTER = date(2026, 4, 8)  # Only process emails from this date onward to avoid old backlog


RESPONSE_UID_PATTERN = compile(r"^\* (?P<uid>\d+) (?P<resp>[A-Z]+).*$")


@handle_fatal_exc_sync
def start_imap_email_monitoring(queue: Queue[MailMessage], loop: AbstractEventLoop) -> None:  # noqa: C901
  """Start the IMAP email monitoring. Runs in a separate thread"""
  # waiting for updates 60 sec, print unseen immediately if any update
  if SETTINGS.realtime_monitor:
    # Third party imports
    from heartrate import trace

    trace(
      port=9998,
      host="127.0.0.1" if __debug__ else "0.0.0.0",
      browser=__debug__,
      daemon=True,
    )

  ssl_context = create_default_context()
  # exists_but_unfound: set[int] = set()

  while True:
    logger.info(f"Emails currently in processing queue: {queue.qsize()}")
    sleep(0)  # Yield control to allow the main thread to run

    logger.info(f"Connecting to IMAP server {SETTINGS.watch_imap_server}:{SETTINGS.watch_imap_port}")
    logger.info(f"  Using email: {SETTINGS.watch_email}")
    try:
      with MailBox(
        host=SETTINGS.watch_imap_server,
        port=SETTINGS.watch_imap_port,
        ssl_context=ssl_context,
      ).login(SETTINGS.watch_email, SETTINGS.watch_email_pwd, "Inbox") as mailbox:
        # Attempting fetch of unfound emails from previous EXISTS responses before polling for new ones
        mailbox.folder.set("Inbox")
        logger.info("Attempting to fetch previously unfound emails")
        for msg in mailbox.fetch(
          A(
            # uid=UidRange("3638", "*"),
            from_="emails@mailing.goftx.com",
            date_gte=STATIC_DATE_FILTER,
            text="report contents",
            no_keyword="AutoMon_Seen",
          )
        ):
          logger.info(f"  Previously unfound email found with UID: {msg.uid}, subject: {msg.subject}. Adding to processing queue.")
          loop.call_soon_threadsafe(queue.put_nowait, msg)
          if msg.uid is not None:
            logger.info(f"  Email with UID {msg.uid} found and added to queue.")

        logger.info("Entering IMAP IDLE mode to wait for new emails...")
        with mailbox.idle as idle:
          logger.info("Polling for new emails...")
          responses = idle.poll(SETTINGS.watch_polling_timeout_sec)

        if FATAL_EVENT.is_set():
          break

        if not responses:
          logger.info(f"no updates in {SETTINGS.watch_polling_timeout_sec} sec\n")
          continue

        logger.info(f"  IMAP IDLE response received: {responses}. Refreshing mailbox")
        mailbox.folder.set("Inbox")

        match = RESPONSE_UID_PATTERN.match(responses[0].decode())
        if match is None:
          logger.error(f"  Received IMAP response did not match expected pattern: {responses[0].decode()}.")
          continue

        logger.info(
          "  Attempting fetch for emails with the following criteria:"
          "    From: emails@mailing.goftx.com\n"
          f"    Date >= {STATIC_DATE_FILTER}\n"
          "    Text contains: 'report contents'\n"
          "    Does not have keyword: 'AutoMon_Seen'"
        )

        # fetch_found = False
        for msg in mailbox.fetch(
          A(
            from_="emails@mailing.goftx.com",
            date_gte=STATIC_DATE_FILTER,
            text="report contents",
            no_keyword="AutoMon_Seen",
          ),
        ):
          # fetch_found = True
          logger.info(f"    New email found with UID: {msg.uid}, subject: {msg.subject}. Adding to processing queue.")
          loop.call_soon_threadsafe(queue.put_nowait, msg)
          if msg.uid is not None:
            flag_as_seen(msg, mailbox)
            # exists_but_unfound.discard(int(msg.uid))  # Remove from unfound list if it was there, no error if it wasn't

        # if not fetch_found:
        #   logger.info("  No matching unseen emails found. Will check again on next IDLE response\n")
        #   exists_but_unfound.add(int(match.group("uid")))

        logger.info("  Finished processing IMAP IDLE response.\n")
    except (socket_error, IMAP4.abort) as e:
      if not isinstance(e.args, tuple) or len(e.args) <= 0 or not isinstance(e.args[0], str) or "EOF" not in e.args[0]:
        # reraise otherwise
        raise e
      logger.warning(f"Socket error occurred (likely due to server closing connection): {e}. Will attempt to reconnect.")


def flag_as_seen(msg: MailMessage, mailbox: MailBox):
  assert msg.uid is not None, "This is impossible."
  logger.info(f"    Flagging {msg.uid} as seen")
  mailbox.flag(msg.uid, "AutoMon_Seen", value=True)
  # mailbox.flag(msg.uid, MailMessageFlags.SEEN, True)
  logger.info(f"    {msg.uid} flagged as seen")
