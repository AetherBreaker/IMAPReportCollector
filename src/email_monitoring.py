if __name__ == "__main__":
  from sys import platform

  from logging_config import configure_logging
  from rich.console import Console

  RICH_CONSOLE = Console(width=None if platform == "win32" else 175, log_time=platform == "win32")

  configure_logging(RICH_CONSOLE)
else:
  from logging_config import RICH_CONSOLE


from asyncio.queues import Queue
from datetime import date
from logging import getLogger
from re import compile
from ssl import create_default_context
from time import sleep

from environment_init_vars import FATAL_EVENT, SETTINGS
from err_handling import handle_fatal_exc_sync
from imap_tools import A, MailBox, MailMessage

logger = getLogger(__name__)


STATIC_DATE_FILTER = date(2026, 4, 8)  # Only process emails from this date onward to avoid old backlog


RESPONSE_UID_PATTERN = compile(r"^\* (?P<uid>\d+) (?P<resp>[A-Z]+).*$")


@handle_fatal_exc_sync
def start_imap_email_monitoring(queue: Queue[MailMessage]) -> None:
  """Start the IMAP email monitoring. Runs in a separate thread"""
  # waiting for updates 60 sec, print unseen immediately if any update
  ssl_context = create_default_context()
  exists_but_unfound: set[int] = set()

  while True:
    logger.info(f"Emails currently in processing queue: {queue.qsize()}")
    sleep(0)  # Yield control to allow the main thread to run

    logger.info("Connecting to IMAP server to check for new emails...")
    with MailBox(
      host=SETTINGS.watch_imap_server,
      port=SETTINGS.watch_imap_port,
      ssl_context=ssl_context,
    ).login(SETTINGS.watch_email, SETTINGS.watch_email_pwd, "Inbox") as mailbox:
      # Attempting fetch of unfound emails from previous EXISTS responses before polling for new ones
      if exists_but_unfound:
        mailbox.folder.set("Inbox")
        logger.info(f"Attempting to fetch previously unfound emails with UIDs: {exists_but_unfound}")
        for uid in exists_but_unfound.copy():  # Iterate over a copy of the set
          logger.info(f"  Attempting direct UID fetch for previously unfound email with UID: {uid}...")
          fetch_found = False
          for msg in mailbox.fetch(
            A(
              from_="emails@mailing.goftx.com",
              date_gte=STATIC_DATE_FILTER,
              text="report contents",
              no_keyword="AutoMon_Seen",
            )
          ):
            fetch_found = True
            if msg.uid is not None:
              flag_as_seen(msg, mailbox)
            logger.info(f"  Previously unfound email found with UID: {msg.uid}, subject: {msg.subject}. Adding to processing queue.")
            queue.put_nowait(msg)
            if msg.uid is not None and int(msg.uid) in exists_but_unfound:
              logger.info(f"  Email with UID {msg.uid} found and added to queue. Removing from unfound list.")
              exists_but_unfound.remove(int(msg.uid))  # Remove from the original list if found
          if not fetch_found:
            logger.info(f"  Email with UID {uid} still not found. Will check again on next IDLE response.")

      logger.info("Entering IMAP IDLE mode to wait for new emails...")
      mailbox.client
      with mailbox.idle as idle:
        logger.info("Polling for new emails...")
        responses = idle.poll(SETTINGS.watch_polling_timeout_sec)

      if FATAL_EVENT.is_set():
        break

      if responses:
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

        fetch_found = False
        for msg in mailbox.fetch(
          A(
            from_="emails@mailing.goftx.com",
            date_gte=STATIC_DATE_FILTER,
            text="report contents",
            no_keyword="AutoMon_Seen",
          ),
        ):
          fetch_found = True
          if msg.uid is not None:
            flag_as_seen(msg, mailbox)
          logger.info(f"    New email found with UID: {msg.uid}, subject: {msg.subject}. Adding to processing queue.")
          queue.put_nowait(msg)

        if not fetch_found:
          logger.info("  No matching unseen emails found. Will check again on next IDLE response\n")
          exists_but_unfound.add(int(match.group("uid")))

        else:
          logger.info("  Finished processing IMAP IDLE response.\n")

      else:
        logger.info(f"no updates in {SETTINGS.watch_polling_timeout_sec} sec\n")


def flag_as_seen(msg: MailMessage, mailbox: MailBox):
  assert msg.uid is not None, "This is impossible."
  logger.info(f"    Flagging {msg.uid} as seen")
  mailbox.flag(msg.uid, "AutoMon_Seen", True)
  # mailbox.flag(msg.uid, MailMessageFlags.SEEN, True)
  logger.info(f"    {msg.uid} flagged as seen")


if __name__ == "__main__":
  test_queue = Queue()
  start_imap_email_monitoring(test_queue)
