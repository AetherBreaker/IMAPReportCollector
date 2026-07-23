# Standard library imports
from collections.abc import Callable
from os import PathLike
from re import Pattern
from typing import Any, Literal

def all(_path: Any) -> Literal[True]: ...  # noqa: A001
def path_contains(*subs: str) -> Callable[[str], bool]: ...
def contains_regex(pattern: str | Pattern[str]) -> Callable[[PathLike | str], bool]: ...
