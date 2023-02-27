from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Protocol, Union

from aiohttp.web_app import Application
from aiohttp.web_response import StreamResponse


ExtendedHandler = Callable[..., Awaitable[StreamResponse]]

CleanupContext = Callable[[Application], AsyncIterator[None]]

PathT = Union[Path, str]

StrDict = dict[str, str]


class HashedFileProtocol(Protocol):
    hash: str
    ext: str
