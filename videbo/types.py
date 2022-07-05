from typing import Any, AsyncIterator, Awaitable, Callable, Tuple, TypeVar

from aiohttp.web_app import Application


AwaitFuncT = Callable[..., Awaitable[Any]]

RouteHandler = TypeVar('RouteHandler', bound=AwaitFuncT)

FileID = Tuple[str, str]  # (hash, file extension)

CleanupContext = Callable[[Application], AsyncIterator[None]]
