from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any, TypeVar, Union

from aiohttp.web_app import Application


AwaitFuncT = Callable[..., Awaitable[Any]]

RouteHandler = TypeVar('RouteHandler', bound=AwaitFuncT)

FileID = tuple[str, str]  # (hash, file extension)

CleanupContext = Callable[[Application], AsyncIterator[None]]

PathT = Union[Path, str]
