if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from asyncio.queues import Queue
from datetime import date
from logging import getLogger
from re import compile, sub
from ssl import create_default_context

from environment_init_vars import FATAL_EVENT, SETTINGS
from err_handling import handle_fatal_exc_sync
from imap_tools import A, MailBox, MailMessage, MailMessageFlags

logger = getLogger(__name__)


STATIC_DATE_FILTER = date(2026, 4, 8)  # Only process emails from this date onward to avoid old backlog


RESPONSE_UID_PATTERN = compile(r"^\* (?P<uid>\d+) .*$")

seen_uids = set()


# @handle_fatal_exc_sync
def start_imap_email_monitoring(
  # queue: Queue[MailMessage],
) -> None:
  """Start the IMAP email monitoring. Runs in a separate thread"""
  # waiting for updates 60 sec, print unseen immediately if any update
  ssl_context = create_default_context()
  with MailBox(
    host=SETTINGS.watch_imap_server,
    port=SETTINGS.watch_imap_port,
    ssl_context=ssl_context,
  ).login(SETTINGS.watch_email, SETTINGS.watch_email_pwd, "Inbox") as mailbox:
    # tmp = True
    while True:
      logger.info("Entering IMAP IDLE mode to wait for new emails...")
      with mailbox.idle as idle:
        logger.info("Polling for new emails...")
        responses = idle.poll(SETTINGS.watch_polling_timeout_sec)
      if FATAL_EVENT.is_set():
        break
      if responses:
        logger.info(f"IMAP IDLE response received: {responses}. Refreshing mailbox")
        mailbox.folder.set("Inbox")

        found = []

        logger.info("Fetching unseen emails matching criteria...")

        # try:
        for msg in mailbox.fetch(
          A(
            seen=False,
            from_="emails@mailing.goftx.com",
            # date_gte=STATIC_DATE_FILTER,
            # text="report contents",
            # new=True,
          ),
        ):
          found.append(msg)
          if msg.uid is not None:
            flag_as_seen(msg, mailbox)
          logger.info(f"New email found with subject: {msg.subject}. Adding to processing queue.")
          # queue.put_nowait(msg)
          # tmp = False
          # break
        if not found:
          logger.info("Fetch failed. Attempting direct uid fetch")

          uid = None
          if match := RESPONSE_UID_PATTERN.match(responses[0].decode()):
            uid = match.group("uid")

          if uid:
            logger.info(f"Extracted UID: {uid}. Fetching email by UID...")
            if messages_iter := mailbox.fetch(A(uid=uid, seen=False)):
              msg = next(messages_iter)  # Get the first (and should be only) message
              if msg.uid is not None:
                flag_as_seen(msg, mailbox)
              logger.info(f"Email fetched by UID with subject: {msg.subject}. Adding to processing queue.")
        else:
          logger.info("Fetch complete")
          # except Exception as e:
          #   logger.exception(f"Error during email fetch: {e}")
      else:
        logger.info(f"no updates in {SETTINGS.watch_polling_timeout_sec} sec")


def flag_as_seen(msg, mailbox):
  logger.info(f"Flagging {msg.uid} as seen")
  mailbox.flag(msg.uid, MailMessageFlags.SEEN, True)
  logger.info(f"{msg.uid} flagged as seen")


if __name__ == "__main__":
  start_imap_email_monitoring()
