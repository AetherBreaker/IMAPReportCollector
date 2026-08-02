# Standard library imports
import sys
from asyncio import Queue, create_task, get_running_loop, run
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from threading import Thread
from typing import TYPE_CHECKING, NoReturn

# Third party imports
from rich import get_console

# First party imports
from aeth_ext.errors import FATAL_EVENT
from aeth_ext.monitoring import run_heartbeat_async
from imap_report_collector.email_monitoring import start_imap_email_monitoring
from imap_report_collector.email_processing import direct_email_processing
from imap_report_collector.environment_init_vars import SETTINGS

if TYPE_CHECKING:
  # Third party imports
  from imap_tools import MailMessage

logger = getLogger(__name__)
RICH_CONSOLE = get_console()

HEARTBEAT_FILE = SETTINGS.log_loc_folder / "heartbeat.txt"


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

  emails_to_process_queue: Queue[MailMessage] = Queue()

  # async with TaskGroup() as main_tasks:
  periodic_heartbeat_task = create_task(
    run_heartbeat_async(
      HEARTBEAT_FILE,
      ping_url=SETTINGS.alerts_healthcheck_ping_url,
      pingkey=SETTINGS.alerts_healthcheck_pingkey,
      tz=SETTINGS.tz,
    )
  )
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


if __name__ == "__main__":
  run(main())
