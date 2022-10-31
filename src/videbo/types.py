from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Union

from aiohttp.web_app import Application
from aiohttp.web_response import StreamResponse


ExtendedHandler = Callable[..., Awaitable[StreamResponse]]

FileID = tuple[str, str]  # (hash, file extension)

CleanupContext = Callable[[Application], AsyncIterator[None]]

PathT = Union[Path, str]
