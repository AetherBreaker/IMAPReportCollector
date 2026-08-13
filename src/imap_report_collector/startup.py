# Standard library imports
import sys
from asyncio import CancelledError, Queue, create_task, get_running_loop, run, run_coroutine_threadsafe
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from logging import getLogger
from threading import Thread
from typing import TYPE_CHECKING, NoReturn

# Third party imports
from rich import get_console

# First party imports
from aeth_ext.errors.shutdown import SHUTDOWN, ShutdownPhase, register_for_shutdown
from aeth_ext.monitoring import run_heartbeat_async
from imap_report_collector.email_monitoring import start_imap_email_monitoring
from imap_report_collector.email_processing import direct_email_processing
from imap_report_collector.environment_init_vars import SETTINGS

if TYPE_CHECKING:
  # Standard library imports
  from asyncio import Task

  # Third party imports
  from imap_tools import MailMessage

logger = getLogger(__name__)
RICH_CONSOLE = get_console()

HEARTBEAT_FILE = SETTINGS.log_loc_folder / "heartbeat.txt"

# The IDLE step now wakes on aeth_ext's SHUTDOWN_WAKEUP directly (see
# email_monitoring._poll_idle_and_process) instead of polling out its own timeout, so it no
# longer contributes to worst-case shutdown latency. Only watch_socket_timeout_sec remains as
# the governing bound -- it covers login/fetch and idle.stop()'s tagged-response wait, all of
# which share the one persistent socket timeout MailBox(...) was given. Bounded rather than
# open-ended so a wedged connection can't consume the whole shutdown budget -- see
# _finish_email_monitoring_shutdown.
EMAIL_MONITORING_JOIN_TIMEOUT_SECS = SETTINGS.watch_socket_timeout_sec + 3

# Defensive bound on how long the threaded shutdown pass waits for
# _shutdown_app_tasks (queue/task/executor teardown) to finish on the loop. Not
# a tight budget -- cancellation itself is near-instant, but a `process_email`
# thread already mid-SFTP-upload when cancelled can't be preempted, only
# awaited out, so this needs enough room for one such upload to unwind rather
# than for the common case.
APP_TASKS_SHUTDOWN_TIMEOUT_SECS = 10.0


async def _await(task: Task[None]) -> None:
  """Adapt a ``Task`` into a plain coroutine, for ``run_coroutine_threadsafe``.

  ``run_coroutine_threadsafe`` requires an actual coroutine object -- a
  ``Task`` (itself only a `Future`-like awaitable, not a coroutine) is
  rejected outright. This is the minimal wrapper that lets a shutdown-thread
  callback block on a task already running on the loop.
  """
  await task


async def _shutdown_app_tasks(
  emails_to_process_queue: Queue[MailMessage],
  email_processing_task: Task[None],
  periodic_heartbeat_task: Task[None],
  executor: ThreadPoolExecutor | None,
) -> None:
  """Wait for SHUTDOWN, then tear down the queue, tasks, and executor.

  Run as its own task from ``main()``, rather than inline after an
  ``await SHUTDOWN`` there, so a second entry point can wait on the exact
  same cleanup: ``main()``'s ``_finish_app_tasks_shutdown`` callback
  schedules a wait on that task onto the loop via ``run_coroutine_threadsafe``
  from aeth_ext's shutdown thread and blocks on the result -- that's what
  makes the threaded shutdown pass actually wait for this work instead of
  racing it before nudging the process to exit (D-I4). ``main()`` also awaits
  the task directly, and must: returning from ``main()`` tears the loop down,
  and nothing else keeps it alive long enough for that cross-thread wait to
  be satisfied.
  """
  try:
    await SHUTDOWN  # Wait for shutdown signal
  except KeyboardInterrupt:
    # aeth_ext's D-I4 early exit nudges the main thread via a simulated SIGINT
    # once the threaded shutdown pass finishes, which can land here rather
    # than after a normal SHUTDOWN wakeup. Falling through to `finally` either
    # way is what keeps cleanup from being skipped depending on the race.
    logger.info("Shutdown requested via interrupt; stopping IMAP report collector")
  finally:
    # with RICH_CONSOLE.status("[bold red]Shutting down...[/]", spinner="dots"):
    RICH_CONSOLE.rule("[bold red]Shutting down...[/]", style="bold red")

    emails_to_process_queue.shutdown()  # Signal that no more emails will be added to the queue

    email_processing_task.cancel()
    with suppress(CancelledError):
      await email_processing_task

    periodic_heartbeat_task.cancel()
    with suppress(CancelledError):
      await periodic_heartbeat_task

    if executor is None:
      await get_running_loop().shutdown_default_executor()
    else:
      executor.shutdown(wait=True)


async def main() -> NoReturn:  # sourcery skip: remove-empty-nested-block

  loop = get_running_loop()

  executor: ThreadPoolExecutor | None = None
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

  email_monitoring_thread = Thread(target=start_imap_email_monitoring, args=(emails_to_process_queue, loop), daemon=True)
  email_monitoring_thread.start()

  def _finish_email_monitoring_shutdown() -> None:
    """Wait for the IMAP monitoring thread to exit its connection (D-I2 threaded phase).

    Registered rather than joined inline in ``main()`` so it is bounded and
    accounted for by aeth_ext's own shutdown budget instead of racing it: with
    nothing registered, the shutdown thread had no pending callbacks and
    nudged the main thread to exit almost immediately after SHUTDOWN was set,
    which could land mid-cleanup here. Not ``required``: the thread is a
    daemon with no buffered writes to flush, so under FORCED it's fine to let
    the process exit without waiting on it.
    """
    email_monitoring_thread.join(timeout=EMAIL_MONITORING_JOIN_TIMEOUT_SECS)
    if email_monitoring_thread.is_alive():
      logger.warning("Email monitoring thread did not shut down within timeout.")

  register_for_shutdown(_finish_email_monitoring_shutdown, phase=ShutdownPhase.THREADED)

  shutdown_task = create_task(_shutdown_app_tasks(emails_to_process_queue, email_processing_task, periodic_heartbeat_task, executor))

  def _finish_app_tasks_shutdown() -> None:
    """Block the threaded shutdown pass on ``_shutdown_app_tasks`` (D-I2 threaded phase).

    ``run_coroutine_threadsafe`` schedules the wait onto the loop from this
    (shutdown) thread; ``.result()`` blocks here until it completes, which is
    what stops aeth_ext's early-exit nudge from firing while
    ``_shutdown_app_tasks`` is still mid-cleanup on the loop thread. Not
    ``required``: none of that cleanup is a durability flush (that's the
    interrupt pass's job), so under FORCED it's fine to let the process exit
    without waiting on it.
    """
    try:
      run_coroutine_threadsafe(_await(shutdown_task), loop).result(timeout=APP_TASKS_SHUTDOWN_TIMEOUT_SECS)
    except TimeoutError:
      logger.warning("App task shutdown did not complete within timeout.")

  register_for_shutdown(_finish_app_tasks_shutdown, phase=ShutdownPhase.THREADED)

  if __debug__:
    pass

  RICH_CONSOLE.rule("[bold red]Boot Done[/]", style="bold red")
  # with RICH_CONSOLE.status("Application is running."):
  await shutdown_task

  sys.exit(1)


if __name__ == "__main__":
  run(main())
