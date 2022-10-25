import os
import re
from asyncio import get_running_loop
from collections.abc import Callable
from inspect import Parameter, isclass, signature
from pathlib import Path
from typing import Any, TypeVar

from videbo.exceptions import InvalidRouteSignature
from videbo.types import RouteHandler
from . import MEGA


T = TypeVar("T")


async def get_free_disk_space(path: str) -> float:
    """Get free disk space at the given path in MB."""
    st = await get_running_loop().run_in_executor(None, os.statvfs, path)
    return st.f_bavail * st.f_frsize / MEGA


def sanitize_filename(filename: str) -> str:
    """
    Returns a sanitized version of `filename` string suitable for file names.

    Removes any character that is not an ASCII letter, digit, hyphen,
    underscore, tilde, comma, semicolon, square bracket, parenthesis, or dot.
    Leaves no more than two consecutive dots in any place of the file name.
    """
    filename = re.sub(r"[^\w \-_~,;\[\]().]", "", filename, flags=re.ASCII)
    filename = re.sub(r"\.{3,}", "..", filename)
    return filename


def rel_path(filename: str) -> Path:
    """
    Returns a relative `Path` instance based on `filename`.

    The path starts with a directory named with the first two characters
    of the file's name followed by the file name itself.
    """
    if len(Path(filename).parts) != 1:
        raise ValueError(f"'{filename}' is not a valid filename")
    return Path(filename[:2], filename)


def get_parameters_of_class(
    function: Callable[..., Any],
    cls: type,
) -> list[Parameter]:
    """
    Returns the parameters of `function` annotated with the class `cls`.

    Args:
        function:
            Any callable with parameter type annotations
        cls:
            A parameter is included in the output, if it is annotated with
            the specified `cls` or a subclass thereof

    Returns:
        List of `inspect.Parameter` instances in the order they were defined
    """
    output = []
    for name, param in signature(function).parameters.items():
        if isclass(param.annotation) and issubclass(param.annotation, cls):
            output.append(param)
    return output


def get_route_model_param(
    route_handler: RouteHandler,
    cls: type[T],
) -> tuple[str, type[T]]:
    """
    Extracts the parameter name and class from a route annotated with `cls`.

    Args:
        route_handler:
            A route handler function with parameter type annotations
        cls:
            Exactly one parameter must be annotated with the
            specified `cls` or a subclass thereof

    Returns:
        2-tuple of the parameter name and class

    Raises:
        `InvalidRouteSignature` if not exactly one matching parameter is found
    """
    params = get_parameters_of_class(route_handler, cls)
    if len(params) == 0:
        raise InvalidRouteSignature(
            f"No parameter of the type `{cls.__name__}` present "
            f"in function `{route_handler.__name__}`."
        )
    if len(params) > 1:
        raise InvalidRouteSignature(
            f"More than one parameter of the type `{cls.__name__}` present "
            f"in function `{route_handler.__name__}`."
        )
    return params[0].name, params[0].annotation
