if __name__ == "__main__":
  from sys import platform

  from rich.console import Console
  from sft_ext.logging.init_logging import init_logging

  RICH_CONSOLE = Console(
    width=None if platform == "win32" else 165,
    log_time=platform == "win32",
  )
  PROJECT_NAME = "IMAPReportCollector"
  LOGGING_TYPE = "daily"

  init_logging()
else:
  from rich import get_console

  RICH_CONSOLE = get_console()


from asyncio import Queue, create_task, get_running_loop, sleep
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from logging import getLogger
from threading import Thread
from typing import NoReturn

from email_monitoring import start_imap_email_monitoring
from email_processing import direct_email_processing
from environment_init_vars import SETTINGS
from imap_tools import MailMessage
from sft_ext.errors.err_handling import FATAL_EVENT, handle_fatal_exc_async

logger = getLogger(__name__)

if not __debug__:
  # Heartbeat file for health checks
  HEARTBEAT_FILE = SETTINGS.log_loc_folder / "heartbeat.txt"

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

  if SETTINGS.realtime_monitor:
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

  email_monitoring_thread = Thread(target=start_imap_email_monitoring, args=(emails_to_process_queue, get_running_loop()))
  email_monitoring_thread.start()

  # imap_idle_task = main_tasks.create_task(to_thread(start_imap_email_monitoring, queue=emails_to_process_queue))

  if __debug__:
    pass

  RICH_CONSOLE.rule("[bold red]Boot Done[/]", style="bold red")
  # with RICH_CONSOLE.status("Application is running."):
  await FATAL_EVENT

  # with RICH_CONSOLE.status("[bold red]Shutting down...[/]", spinner="dots"):
  email_monitoring_thread.join(60)
  if email_monitoring_thread.is_alive():
    logger.warning("Email monitoring thread did not shut down within timeout.")

  emails_to_process_queue.shutdown()  # Signal that no more emails will be added to the queue

  email_processing_task.cancel()

  periodic_heartbeat_task.cancel()

  if SETTINGS.realtime_monitor:
    executor.shutdown(wait=True)  # type: ignore

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
