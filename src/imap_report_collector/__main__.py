if __name__ == "__main__":
  # Standard library imports
  from sys import platform

  # Third party imports
  from rich.console import Console

  # First party imports
  from aeth_ext import initialize

  RICH_CONSOLE = Console(
    width=None if platform == "win32" else 165,
    log_time=platform == "win32",
  )
  PROJECT_NAME = "imap-report-collector"
  LOGGING_TYPE = "daily"

  initialize(asyncio=True, logging="socket")
else:
  # Third party imports
  from rich import get_console

  RICH_CONSOLE = get_console()

# Standard library imports
import sys
from asyncio import Queue, create_task, get_running_loop, run, sleep
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from logging import getLogger
from threading import Thread
from typing import TYPE_CHECKING, NoReturn

# First party imports
from aeth_ext.errors import FATAL_EVENT, handle_fatal_exc_async
from imap_report_collector.email_monitoring import start_imap_email_monitoring
from imap_report_collector.email_processing import direct_email_processing
from imap_report_collector.environment_init_vars import SETTINGS

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Callable

  # Third party imports
  from imap_tools import MailMessage

logger = getLogger(__name__)

if not __debug__:
  # Heartbeat file for health checks
  HEARTBEAT_FILE = SETTINGS.log_loc_folder / "heartbeat.txt"

  def write_heartbeat():
    """Write current timestamp to heartbeat file for health monitoring."""
    try:
      HEARTBEAT_FILE.write_text(datetime.now(SETTINGS.tz).isoformat())
    except Exception as e:
      logger.error("Failed to write heartbeat: %s", e)
else:

  def write_heartbeat():
    pass


@handle_fatal_exc_async
async def run_periodic(interval: float, func: Callable[[], None]) -> NoReturn:
  """Run a function periodically at a specified interval."""
  while True:
    try:
      func()
    except Exception as e:
      logger.error("Error in periodic task: %s", e)
    await sleep(interval)


async def main() -> NoReturn:  # sourcery skip: remove-empty-nested-block

  if SETTINGS.realtime_monitor:
    # Third party imports
    from heartrate import files, trace

    trace(
      files=files.all,
      port=9999,
      host="127.0.0.1" if __debug__ else "0.0.0.0",
      browser=__debug__,
      daemon=True,
    )

    loop = get_running_loop()

    executor = ThreadPoolExecutor(
      initializer=trace,
      initargs=(
        files.all,
        9997,
        "127.0.0.1" if __debug__ else "0.0.0.0",
        __debug__,
        True,
      ),
    )
    loop.set_default_executor(executor)

  RICH_CONSOLE.rule("[bold red]Booting...[/]", style="bold red")
  # Write initial heartbeat on startup
  write_heartbeat()

  emails_to_process_queue: Queue[MailMessage] = Queue()

  # async with TaskGroup() as main_tasks:
  periodic_heartbeat_task = create_task(run_periodic(30, write_heartbeat))
  email_processing_task = create_task(direct_email_processing(emails_to_process_queue))

  email_monitoring_thread = Thread(target=start_imap_email_monitoring, args=(emails_to_process_queue, get_running_loop()), daemon=True)
  email_monitoring_thread.start()

  if __debug__:
    pass

  RICH_CONSOLE.rule("[bold red]Boot Done[/]", style="bold red")
  # with RICH_CONSOLE.status("Application is running."):
  await FATAL_EVENT

  # with RICH_CONSOLE.status("[bold red]Shutting down...[/]", spinner="dots"):
  RICH_CONSOLE.rule("[bold red]Shutting down...[/]", style="bold red")
  email_monitoring_thread.join(60)
  if email_monitoring_thread.is_alive():
    logger.warning("Email monitoring thread did not shut down within timeout.")

  emails_to_process_queue.shutdown()  # Signal that no more emails will be added to the queue

  email_processing_task.cancel()

  periodic_heartbeat_task.cancel()

  if SETTINGS.realtime_monitor:
    executor.shutdown(wait=True)  # pyright: ignore[reportPossiblyUnboundVariable]

  sys.exit(1)


def run_app() -> None:
  run(main())


if __name__ == "__main__":
  run_app()
