# Standard library imports
from asyncio import run
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
TESTING = True

initialize(asyncio=True, logging="socket")


def run_app() -> None:
  """Run the IMAP Report Collector application."""
  # First party imports
  from imap_report_collector.startup import main

  run(main())


if __name__ == "__main__":
  run_app()
