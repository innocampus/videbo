import os
import re
from asyncio import gather, get_running_loop
from collections.abc import Awaitable, Callable
from inspect import Parameter, isclass, signature
from pathlib import Path
from typing import Any, Type, TypeVar

from pydantic import BaseModel

from videbo.exceptions import InvalidRouteSignature
from videbo.types import RouteHandler
from . import MEGA


M = TypeVar('M', bound=BaseModel)


async def gather_in_batches(batch_size: int, *aws: Awaitable[Any], return_exceptions: bool = False) -> list[Any]:
    results = []
    for idx in range(0, len(aws), batch_size):
        results += await gather(*aws[idx:idx + batch_size], return_exceptions=return_exceptions)
    return results


def ensure_url_does_not_end_with_slash(url: str) -> str:
    while url:
        if url[-1] == '/':
            url = url[0:-1]
        else:
            return url
    return url


async def get_free_disk_space(path: str) -> float:
    """Get free disk space in the given path. Returns MB."""
    st = await get_running_loop().run_in_executor(None, os.statvfs, path)
    free_bytes = st.f_bavail * st.f_frsize
    return free_bytes / MEGA


def sanitize_filename(filename: str) -> str:
    filename = re.sub(r"[^\w \-_~,;\[\]().]", "", filename, 0, re.ASCII)  # \w should only match ASCII letters
    filename = re.sub(r"[.]{2,}", "..", filename)
    return filename


def rel_path(filename: str) -> Path:
    """
    Returns a relative path from a file's name.
    The path starts with a directory named with the first two characters of the filename followed by the file itself.
    """
    if len(Path(filename).parts) != 1:
        raise ValueError(f"'{filename}' is not a valid filename")
    return Path(filename[:2], filename)


def get_parameters_of_type(function: Callable[..., Any], cls: type) -> list[Parameter]:
    output = []
    param: Parameter
    for name, param in signature(function).parameters.items():
        if isclass(param.annotation) and issubclass(param.annotation, cls):
            output.append(param)
    return output


def get_route_model_param(route_handler: RouteHandler, model: Type[M]) -> tuple[str, Type[M]]:
    params = get_parameters_of_type(route_handler, model)
    if len(params) == 0:
        raise InvalidRouteSignature(f"No parameter of the type `{model.__name__}` present in function "
                                    f"`{route_handler.__name__}`.")
    if len(params) > 1:
        raise InvalidRouteSignature(f"More than one parameter of the type `{model.__name__}` present in function "
                                    f"`{route_handler.__name__}`.")
    return params[0].name, params[0].annotation
