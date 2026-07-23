# Standard library imports
from collections.abc import Callable
from os import PathLike

def trace(
  files: Callable[[PathLike | str], bool] = ...,
  port: int = 9999,
  host: str = "127.0.0.1",
  browser: bool = False,
  daemon: bool = False,
): ...
