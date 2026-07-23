# heartrate
if __name__ == "__main__":
  # First party imports
  from aeth_ext import initialize

  initialize()

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
from aeth_ext.errors import FATAL_EVENT, handle_fatal_exc_sync

# Local folder imports
from .environment_init_vars import SETTINGS

if TYPE_CHECKING:
  # Standard library imports
  from asyncio import AbstractEventLoop
  from asyncio.queues import Queue


logger = getLogger(__name__)


STATIC_DATE_FILTER = date(2026, 4, 8)  # Only process emails from this date onward to avoid old backlog


RESPONSE_UID_PATTERN = compile(r"^\* (?P<uid>\d+) (?P<resp>[A-Z]+).*$")


def _setup_heartrate_tracing() -> None:
  """Configure heartrate execution tracing."""
  # Third party imports
  from heartrate import trace

  trace(
    port=9998,
    host="127.0.0.1" if __debug__ else "0.0.0.0",
    browser=__debug__,
    daemon=True,
  )


def _fetch_new_emails(mailbox: MailBox, queue: Queue[MailMessage], loop: AbstractEventLoop) -> None:
  """Fetch newly arrived emails triggered by an IMAP IDLE response and enqueue them."""
  logger.info(
    "  Attempting fetch for emails with the following criteria:"
    "    From: emails@mailing.goftx.com\n"
    "    Date >= %s\n"
    "    Text contains: 'report contents'\n"
    "    Does not have keyword: 'AutoMon_Seen'",
    STATIC_DATE_FILTER,
  )
  logger.info("  Refreshing mailbox")
  mailbox.folder.set("Inbox")
  for msg in mailbox.fetch(
    A(
      from_="emails@mailing.goftx.com",
      date_gte=STATIC_DATE_FILTER,
      text="report contents",
      no_keyword="AutoMon_Seen",
    ),
  ):
    logger.info("    New email found with UID: %s, subject: %s. Adding to processing queue.", msg.uid, msg.subject)
    loop.call_soon_threadsafe(queue.put_nowait, msg)
    if msg.uid is not None:
      flag_as_seen(msg, mailbox)


def _poll_idle_and_process(mailbox: MailBox, queue: Queue[MailMessage], loop: AbstractEventLoop):
  """Enter IMAP IDLE, wait for a server push, and process the response. Returns True if the outer loop should break."""
  logger.info("Entering IMAP IDLE mode to wait for new emails...")
  with mailbox.idle as idle:
    logger.info("Polling for new emails...")
    responses = idle.poll(SETTINGS.watch_polling_timeout_sec)

  if responses:
    logger.info("  IMAP IDLE response received: %s", responses)

    match_result = RESPONSE_UID_PATTERN.match(responses[0].decode())
    if match_result is None:
      logger.error("  Received IMAP response did not match expected pattern: %s.", responses[0].decode())

    _fetch_new_emails(mailbox, queue, loop)
    logger.info("  Finished processing IMAP IDLE response.\n")
  else:
    logger.info("no updates in %s sec", SETTINGS.watch_polling_timeout_sec)


@handle_fatal_exc_sync
def start_imap_email_monitoring(queue: Queue[MailMessage], loop: AbstractEventLoop) -> None:
  """Start the IMAP email monitoring. Runs in a separate thread"""
  # waiting for updates 60 sec, print unseen immediately if any update
  if SETTINGS.realtime_monitor:
    _setup_heartrate_tracing()

  ssl_context = create_default_context()

  not_broken = True

  while not_broken:
    logger.info("Emails currently in processing queue: %s", queue.qsize())
    sleep(0)  # Yield control to allow the main thread to run

    logger.info("Connecting to IMAP server %s:%s", SETTINGS.watch_imap_server, SETTINGS.watch_imap_port)
    logger.info("  Using email: %s", SETTINGS.watch_email)
    try:
      with MailBox(
        host=SETTINGS.watch_imap_server,
        port=SETTINGS.watch_imap_port,
        ssl_context=ssl_context,
      ).login(SETTINGS.watch_email, SETTINGS.watch_email_pwd, "Inbox") as mailbox:
        logger.info("Attempting to fetch previously unfound emails")
        _fetch_new_emails(mailbox, queue, loop)
        _poll_idle_and_process(mailbox, queue, loop)

      if FATAL_EVENT.is_set():
        logger.info("Fatal event detected. Exiting IMAP email monitoring loop.")
        not_broken = False

    except ConnectionRefusedError as e:
      logger.warning("Connection refused error occurred: %s. Will attempt to reconnect.", e)

    except IMAP4.abort as e:
      logger.warning("IMAP4 abort error occurred: %s. Will attempt to reconnect.", e)

    except socket_error as e:
      if not isinstance(e.args, tuple) or len(e.args) <= 0 or not isinstance(e.args[0], str) or "EOF" not in e.args[0]:  # pyright: ignore[reportUnnecessaryIsInstance]
        # reraise otherwise
        raise e
      logger.warning("Socket error occurred (likely due to server closing connection): %s. Will attempt to reconnect.", e)


def flag_as_seen(msg: MailMessage, mailbox: MailBox):
  assert msg.uid is not None, "This is impossible."
  logger.info("    Flagging %s as seen", msg.uid)
  mailbox.flag(msg.uid, "AutoMon_Seen", value=True)
  logger.info("    %s flagged as seen", msg.uid)
