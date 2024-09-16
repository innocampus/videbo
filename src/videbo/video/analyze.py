from asyncio.exceptions import TimeoutError as AsyncioTimeoutError
from asyncio.tasks import gather, wait_for
from logging import Logger, getLogger
from pathlib import Path
from subprocess import PIPE
from typing import Optional, TypedDict

from pydantic import ValidationError

from videbo import settings
from videbo.exceptions import (
    FFMpegError,
    FFProbeError,
    FileCmdError,
    MimeTypeNotAllowed,
    VideoNotAllowed,
)
from videbo.misc.constants import JPG_EXT
from videbo.misc.functions import (
    copy_file,
    create_user_subprocess,
    run_in_default_executor,
)
from videbo.types import PathT
from .models import VideoInfo


__all__ = [
    "get_video_mime_type",
    "get_ffprobe_info",
    "get_video_info",
    "create_thumbnail",
    "create_thumbnail_securely",
    "generate_thumbnails",
]

_log = getLogger(__name__)

DEFAULT_SUBPROCESS_TIMEOUT: float = 10.0  # seconds


# TODO(daniil-berg): Remove the `sudo_user` argument everywhere.
#                    https://github.com/innocampus/videbo/issues/18


async def get_video_mime_type(
    file_path: PathT,
    sudo_user: Optional[str] = None,
    binary_file: Optional[str] = None,
    timeout_seconds: float = DEFAULT_SUBPROCESS_TIMEOUT,
) -> str:
    """
    Determines and returns MIME type of the specified file.

    Calls the `file` program in a subprocess.

    Args:
        file_path:
            Path to the file for which to determine the MIME type
        sudo_user (optional):
            If set, the program will be called with `sudo -u <sudo_user>`;
            defaults to `settings.video.check_user`.
        binary_file (optional):
            Path to `file` binary; defaults to `settings.video.binary_file`
        timeout_seconds (optional):
            Maximum number of seconds to wait for the subprocess to finish;
            if exceeded, a `FileCmdError` will be raised;
            defaults to `DEFAULT_SUBPROCESS_TIMEOUT`.

    Returns:
        The MIME type of the file at `file_path`

    Raises:
        `FileCmdError` if timeout is exceeded
    """
    args = ("-b", "-i", str(file_path))
    proc = await create_user_subprocess(
        binary_file or settings.video.binary_file,
        *args,
        sudo_user=sudo_user or settings.video.check_user,
        stdout=PIPE,
    )
    try:
        stdout, _ = await wait_for(proc.communicate(), timeout_seconds)
    except AsyncioTimeoutError:
        proc.kill()
        raise FileCmdError(timeout=True) from None
    # Strip linebreaks, lowercase, extract first part:
    return stdout.decode().strip().lower().split(";")[0]


async def get_ffprobe_info(
    video_path: PathT,
    sudo_user: Optional[str] = None,
    binary_ffprobe: Optional[str] = None,
    timeout_seconds: float = DEFAULT_SUBPROCESS_TIMEOUT,
) -> VideoInfo:
    """
    Returns `VideoInfo` instance constructed from the specified video file.

    Calls `ffprobe` in a subprocess.

    Args:
        video_path:
            Path to the video file to probe
        sudo_user (optional):
            If set, the program will be called with `sudo -u <sudo_user>`;
            defaults to `settings.video.check_user`.
        binary_ffprobe (optional):
            Path to `ffprobe`; defaults to `settings.video.binary_ffprobe`
        timeout_seconds (optional):
            Maximum number of seconds to wait for the subprocess to finish;
            if exceeded, a `FFProbeError` will be raised;
            defaults to `DEFAULT_SUBPROCESS_TIMEOUT`.

    Returns:
        `VideoInfo` object for the video at `video_path`

    Raises:
        `FFProbeError` if timeout is exceeded or model validation fails
    """
    args = (
        "-show_format",
        "-show_streams",
        "-print_format",
        "json",
        str(video_path),
    )
    proc = await create_user_subprocess(
        binary_ffprobe or settings.video.binary_ffprobe,
        *args,
        sudo_user=sudo_user or settings.video.check_user,
        stdout=PIPE,
        stderr=PIPE,
    )
    try:
        stdout, stderr = await wait_for(proc.communicate(), timeout_seconds)
    except AsyncioTimeoutError:
        proc.kill()
        raise FFProbeError(timeout=True) from None
    try:
        return VideoInfo.parse_raw(stdout.decode())
    except ValidationError:
        # TODO(daniil-berg): Ensure validation problems are properly logged
        #                    https://github.com/innocampus/videbo/issues/12
        raise FFProbeError(stderr=stderr.decode()) from None


async def get_video_info(path: PathT, log: Logger = _log) -> VideoInfo:
    """
    Runs metadata checks on the video at `path` and returns the information.

    Convenience function calling `get_video_mime_type` and `get_ffprobe_info`
    with default arguments before running checks.
    Logs failed checks as expressive warnings.

    Args:
        path: Path to the video file to analyze
        log (optional): Logger instance to use (defaults to the module log)

    Returns:
        Instance of `VideoInfo` if the checks were passed

    Raises:
        `MimeTypeNotAllowed` if the video's MIME type is not whitelisted
        `FFProbeError` if analysis with `ffprobe` could not be completed
        `VideoNotAllowed` if the video's codec or format is not whitelisted
    """
    mime_type = await get_video_mime_type(path)
    if mime_type not in settings.video.mime_types_allowed:
        exception = MimeTypeNotAllowed(mime_type)
        log.warning(repr(exception))
        raise exception
    try:
        probe_info = await get_ffprobe_info(path)
    except (FFProbeError, VideoNotAllowed) as e:
        log.warning(repr(e))
        raise
    return probe_info


class CreateThumbnailKwargs(TypedDict, total=False):
    """Should match the signature of `create_thumbnail` below."""
    video_path: PathT
    thumbnail_dst: PathT
    offset: int
    height: int
    sudo_user: Optional[str]
    binary_ffmpeg: Optional[str]
    timeout_seconds: float


async def create_thumbnail(
    video_path: PathT,
    thumbnail_dst: PathT,
    offset: int,
    height: int,
    sudo_user: Optional[str] = None,
    binary_ffmpeg: Optional[str] = None,
    timeout_seconds: float = DEFAULT_SUBPROCESS_TIMEOUT,
) -> None:
    """
    Creates a scaled thumbnail from a frame of the specified video.

    Calls `ffmpeg` in a subprocess.

    Args:
        video_path:
            Path to the video file for which to create the thumbnail
        thumbnail_dst:
            Desired output path for the thumbnail
        offset:
            The offset at which to take the frame for the thumbnail
        height:
            The height to which to scale the thumbnail
        sudo_user (optional):
            If set, the program will be called with `sudo -u <sudo_user>`;
            defaults to `settings.video.check_user`.
        binary_ffmpeg (optional):
            Path to `ffmpeg`; defaults to `settings.video.binary_ffmpeg`
        timeout_seconds (optional):
            Maximum number of seconds to wait for the subprocess to finish;
            if exceeded, a `FFMpegError` will be raised;
            defaults to `DEFAULT_SUBPROCESS_TIMEOUT`.

    Raises:
        `FFMpegError` if timeout is exceeded or model validation fails
    """
    args = (
        "-ss",
        str(offset),
        "-i",
        str(video_path),
        "-vframes",
        "1",
        "-an",
        "-vf",
        f"scale=-1:{height}",
        "-y",
        str(thumbnail_dst),
    )
    proc = await create_user_subprocess(
        binary_ffmpeg or settings.video.binary_ffmpeg,
        *args,
        sudo_user=sudo_user or settings.video.check_user,
    )
    try:
        await wait_for(proc.wait(), timeout_seconds)
    except AsyncioTimeoutError:
        proc.kill()
        raise FFMpegError(timeout=True) from None


async def create_thumbnail_securely(
    video_path: PathT,
    thumbnail_dst: PathT,
    offset: int,
    height: int,
    interim_path: PathT,
    sudo_user: Optional[str] = None,
    binary_ffmpeg: Optional[str] = None,
    timeout_seconds: float = DEFAULT_SUBPROCESS_TIMEOUT,
) -> None:
    """
    Creates a thumbnail via an interim path.

    It is created at `interim_path` and then copied to `thumbnail_dst`;
    the `interim_path` is always removed in the end.

    For more details see `create_thumbnail`.
    """
    try:
        await create_thumbnail(
            video_path,
            interim_path,
            offset,
            height,
            sudo_user=sudo_user,
            binary_ffmpeg=binary_ffmpeg,
            timeout_seconds=timeout_seconds,
        )
        await run_in_default_executor(copy_file, interim_path, thumbnail_dst)
    finally:
        await run_in_default_executor(Path(interim_path).unlink, True)


async def generate_thumbnails(
    video_path: PathT,
    video_duration: float,
    interim_dir: Optional[PathT] = None,
    height: Optional[int] = None,
    count: Optional[int] = None,
    sudo_user: Optional[str] = None,
    binary_ffmpeg: Optional[str] = None,
    timeout_seconds: float = DEFAULT_SUBPROCESS_TIMEOUT,
) -> int:
    """
    Generates scaled thumbnails from frames of the specified video.

    The exact frame offset used for each thumbnail depends on the specified
    `video_duration` and the desired thumbnail `count`.
    Frames will be evenly spaced across the length of the video.

    The thumbnails will be placed in the same directory as the `video_path`
    and their file names will have the same stem as the `video_path`, but
    with `_<thumbnail_number>` appended to it, and the `JPG_EXT` suffix.

    If `interim_dir` is set, `create_thumbnail_securely` is used.

    Thumbnails are created and saved (and if necessary copied) concurrently.

    For more details see `create_thumbnail` and `create_thumbnail_securely`.
    """
    video_path = Path(video_path)
    count = count or settings.thumbnails.suggestion_count
    kwargs = CreateThumbnailKwargs(
        video_path=video_path,
        height=int(height or settings.thumbnails.height),
        sudo_user=sudo_user,
        binary_ffmpeg=binary_ffmpeg,
        timeout_seconds=timeout_seconds,
    )
    coroutines = []
    for n in range(count):
        file_stem = f"{video_path.stem}_{n}"
        thumbnail_dst = video_path.with_stem(file_stem).with_suffix(JPG_EXT)
        kwargs["thumbnail_dst"] = thumbnail_dst
        kwargs["offset"] = int(video_duration / count * (n + 0.5))
        if interim_dir is None:
            coroutines.append(create_thumbnail(**kwargs))
        else:
            interim_path = Path(interim_dir, thumbnail_dst.name)
            coroutines.append(
                create_thumbnail_securely(**kwargs, interim_path=interim_path)
            )
    await gather(*coroutines)
    return count
