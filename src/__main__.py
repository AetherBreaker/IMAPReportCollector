if __name__ == "__main__":
  from sys import platform

  from logging_config import configure_logging
  from rich.console import Console

  RICH_CONSOLE = Console(
    width=None if platform == "win32" else 175,
    log_time=platform == "win32",
    force_interactive=False,
  )

  configure_logging(RICH_CONSOLE)
else:
  from logging_config import RICH_CONSOLE


from asyncio import TaskGroup, sleep, to_thread
from asyncio.queues import Queue
from collections.abc import Callable
from datetime import datetime
from logging import getLogger
from pathlib import PosixPath, PurePosixPath
from typing import NoReturn

from email_monitoring import start_imap_email_monitoring
from email_processing import direct_email_processing
from environment_init_vars import FATAL_EVENT
from err_handling import handle_fatal_exc_async
from imap_tools import MailMessage
from logging_config import RICH_CONSOLE

logger = getLogger(__name__)

if not __debug__:
  # Heartbeat file for health checks
  HEARTBEAT_FILE = PurePosixPath("/app/src/logs/heartbeat.txt") if __debug__ else PosixPath("/app/src/logs/heartbeat.txt")

  def write_heartbeat():
    """Write current timestamp to heartbeat file for health monitoring."""
    try:
      HEARTBEAT_FILE.write_text(datetime.now().isoformat())  # type: ignore
    except Exception as e:
      logger.error(f"Failed to write heartbeat: {e}")
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
      logger.error(f"Error in periodic task: {e}")
    await sleep(interval)


async def main() -> NoReturn:  # sourcery skip: remove-empty-nested-block
  RICH_CONSOLE.rule("[bold red]Booting...[/]", style="bold red")
  # Write initial heartbeat on startup
  write_heartbeat()

  emails_to_process_queue: Queue[MailMessage] = Queue()

  async with TaskGroup() as main_tasks:
    periodic_heartbeat_task = main_tasks.create_task(run_periodic(30, write_heartbeat))
    email_processing_task = main_tasks.create_task(direct_email_processing(emails_to_process_queue))
    imap_idle_task = main_tasks.create_task(to_thread(start_imap_email_monitoring, queue=emails_to_process_queue))

    if __debug__:
      pass

    RICH_CONSOLE.rule("[bold red]Boot Done[/]", style="bold red")
    # with RICH_CONSOLE.status("Application is running."):
    await FATAL_EVENT

    with RICH_CONSOLE.status("[bold red]Shutting down...[/]", spinner="dots"):
      email_processing_task.cancel()

      imap_idle_task.cancel()

      periodic_heartbeat_task.cancel()

  emails_to_process_queue.shutdown(immediate=True)

  exit(1)

  raise RuntimeError("How did we get here? The main function should never exit normally.")


if __name__ == "__main__":
  from sys import platform

  if platform in ("win32", "cygwin", "cli"):
    from winloop import run
  else:
    # if we're on apple or linux do this instead
    from uvloop import run  # type: ignore
  run(main())
