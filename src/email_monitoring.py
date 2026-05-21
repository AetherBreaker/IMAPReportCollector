if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import asyncio
from asyncio.queues import Queue
from logging import getLogger
from ssl import create_default_context

from environment_init_vars import FATAL_EVENT, SETTINGS
from err_handling import handle_fatal_exc_sync
from imap_tools import A, MailBox, MailMessage, MailMessageFlags

logger = getLogger(__name__)


@handle_fatal_exc_sync
def start_imap_email_monitoring(
  queue: Queue[MailMessage],
  loop: asyncio.AbstractEventLoop,
) -> None:
  """Start the IMAP email monitoring. Runs in a separate thread."""
  ssl_context = create_default_context()
  with MailBox(
    host=SETTINGS.watch_imap_server,
    port=SETTINGS.watch_imap_port,
    ssl_context=ssl_context,
  ).login(SETTINGS.watch_email, SETTINGS.watch_email_pwd, "Inbox") as mailbox:
    while True:
      logger.info("Entering IMAP IDLE mode to wait for new emails...")
      with mailbox.idle as idle:
        logger.info("Polling for new emails...")
        responses = idle.poll(SETTINGS.watch_polling_timeout_sec)
      if FATAL_EVENT.is_set():
        break
      if not responses:
        logger.info(f"no updates in {SETTINGS.watch_polling_timeout_sec} sec")
        continue

      logger.info(f"IMAP IDLE response received: {responses}. Refreshing mailbox")
      mailbox.folder.set("Inbox")

      # IDLE notification is just a wake-up signal. Numbers in IDLE responses
      # (e.g. "* 5 EXISTS") are sequence numbers / mailbox counts, NOT UIDs.
      # Re-search for unseen Control Center reports and queue them.
      logger.info("Fetching unseen Control Center emails...")
      found = 0
      for msg in mailbox.fetch(A(seen=False, from_="emails@mailing.goftx.com"), mark_seen=False):
        found += 1
        logger.info(f"New email from {msg.from_} - subject: {msg.subject}. Adding to processing queue.")
        loop.call_soon_threadsafe(queue.put_nowait, msg)
        if msg.uid is not None:
          flag_as_seen(msg, mailbox)

      if found:
        logger.info(f"Fetch complete — queued {found} email(s)")
      else:
        logger.info("IDLE wake but no unseen Control Center emails matched (likely an unrelated email or flag change)")


def flag_as_seen(msg, mailbox):
  logger.info(f"Flagging {msg.uid} as seen")
  mailbox.flag(msg.uid, MailMessageFlags.SEEN, True)
  logger.info(f"{msg.uid} flagged as seen")
