import os
import re
from asyncio import get_running_loop
from asyncio.subprocess import Process
from asyncio.subprocess import create_subprocess_exec as _create_subproc
from collections.abc import Callable
from inspect import Parameter, signature
from pathlib import Path
from shutil import copyfile as _shutil_copyfile
from subprocess import DEVNULL
from typing import Any, Optional, TypeVar, Union
from typing_extensions import TypeGuard

from videbo.exceptions import InvalidRouteSignature
from videbo.types import ExtendedHandler, PathT
from .constants import MEGA, VIDEO_FILE_EXT_MIME_TYPE

__all__ = [
    "run_in_default_executor",
    "get_free_disk_space",
    "move_file",
    "copy_file",
    "sanitize_filename",
    "rel_path",
    "get_parameters_of_class",
    "get_route_model_param",
    "create_user_subprocess",
]


T = TypeVar("T")


async def run_in_default_executor(func: Callable[..., T], *args: Any) -> T:
    """Runs `func(*args)` in the running event loop's default executor."""
    return await get_running_loop().run_in_executor(None, func, *args)


async def get_free_disk_space(path: str) -> float:
    """Get free disk space at the given path in MB."""
    st = await run_in_default_executor(os.statvfs, path)
    return st.f_bavail * st.f_frsize / MEGA


def move_file(
    src: PathT,
    dst: PathT,
    create_dir_mode: Optional[int] = None,
    chmod: int = 0o644,
    safe: bool = False,
) -> bool:
    """
    Moves a file from on location in the filesystem to another.

    Args:
        src:
            The file to move
        dst:
            The desired new location of the file
        create_dir_mode (optional):
            If provided and the parent/directory of the destination file path
            is missing, it is first created with the specified permissions.
        chmod (optional):
            The permissions to set on the destination file path;
            defaults to `0o644`.
        safe (optional):
            If `True` and the destination path already exists and is a file,
            a `FileExistsError` is raised; if `False` (default) and the
            destination file already exists, the source file just is removed.

    Returns:
        `True` if the `dst` path did not exist yet and `src` file was moved;
        `False` if `dst` already existed and `src` file was merely deleted.
    """
    dst = Path(dst)
    if create_dir_mode is not None:
        dst.parent.mkdir(mode=create_dir_mode, parents=True, exist_ok=True)
    if dst.is_file():
        if safe:
            raise FileExistsError
        Path(src).unlink()
        return False
    else:  # move
        Path(src).rename(dst)
        dst.chmod(chmod)
        return True


def copy_file(src: PathT, dst: PathT, overwrite: bool = False) -> bool:
    """
    Copies a file from one location in the filesystem to another.

    Args:
        src:
            The file to copy
        dst:
            The desired new location of the file
        overwrite (optional):
            If `True` and the `dst` path already exists and is a file,
            it will be overwritten with `src`; if `False` (default) and the
            `dst` path already exists and is a file, no action is performed.

    Returns:
        `True` if the `src` file was copied; `False` otherwise.
    """
    if overwrite or not Path(dst).is_file():
        _shutil_copyfile(src, dst)
        return True
    return False


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


def mime_type_from_file_name(file: PathT, strict: bool = False) -> str:
    """
    Returns the appropriate MIME type based on a the extension of a file.

    Will only return an official IANA registered media type.
    (Ref. https://www.iana.org/assignments/media-types/media-types.xhtml)

    NOTE: Only handles video files!

    Args:
        file:
            Either a string or `Path` object containing the file name
        strict (optional):
            If `True` and no matching MIME type is found, an error is raised;
            if `False` (default) and no matching MIME type is found, the
            generic `"application/octet-stream"` is returned.

    Returns:
        A string of the MIME type matching the given file's extension.

    Raises:
        `ValueError` if `strict` is `True` and no matching MIME type is found
    """
    ext = Path(file).suffix[1:]
    mime_type = VIDEO_FILE_EXT_MIME_TYPE.get(ext)
    if mime_type is not None:
        return mime_type
    if strict:
        raise ValueError(f"Unrecognized file extension: {ext}")
    return "application/octet-stream"


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
    for param in signature(function).parameters.values():
        if is_subclass(param.annotation, cls):
            output.append(param)
    return output


def get_route_model_param(
    route_handler: ExtendedHandler,
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


async def create_user_subprocess(
    program: str,
    *args: str,
    sudo_user: Optional[str] = None,
    **kwargs: Any,
) -> Process:
    """
    Creates an `asyncio.subprocess.Process` with the provided arguments.

    If `sudo_user` is specified, the program will be called with
    `sudo -u <sudo_user>`, which requires `sudo` to be available and the
    appropriate settings in the sudoers file to be set.

    For the other arguments see `create_subprocess_exec` documentation:
    https://docs.python.org/3/library/asyncio-subprocess.html

    By default `stdout` and `stderr` are both set to `subprocess.DEVNULL`.
    """
    if sudo_user:
        args = ("-u", sudo_user, program) + args
        program = "sudo"
    kwargs.setdefault("stdout", DEVNULL)
    kwargs.setdefault("stderr", DEVNULL)
    return await _create_subproc(program, *args, **kwargs)


def is_subclass(
    __cls: object,
    __class_or_tuple: Union[type[T], tuple[type[T], ...]],
) -> TypeGuard[type[T]]:
    """More lenient version of the built-in `issubclass` function."""
    if not isinstance(__cls, type):
        return False
    return issubclass(__cls, __class_or_tuple)
