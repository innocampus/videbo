from enum import Enum
from logging import getLogger
from math import isnan, nan
from pathlib import Path
from typing import Any, Optional
from typing_extensions import assert_never

from pydantic import BaseModel, validator

from videbo import settings


__all__ = [
    "FFProbeFormat",
    "FFProbeStream",
    "VideoInfo",
]

_log = getLogger(__name__)


class CodecType(str, Enum):
    audio = 'audio'
    video = 'video'
    data = 'data'  # e.g. timecode stream


class FFProbeStream(BaseModel):
    """Relevant parts of the `stream` section of a `ffprobe` output."""

    index: int
    codec_name: Optional[str]
    codec_type: CodecType
    duration: float = nan

    @validator("codec_type")
    def ensure_whitelisted_codec(
        cls,
        v: CodecType,
        values: dict[str, Any],
    ) -> CodecType:
        """Ensures video/audio codec is whitelisted in settings."""
        codec_name = values.get("codec_name")
        if v is CodecType.video:
            if codec_name in settings.video.video_codecs_allowed:
                return v
            raise ValueError(f"Video codect not allowed: {codec_name}")
        if v is CodecType.audio:
            if codec_name in settings.video.audio_codecs_allowed:
                return v
            raise ValueError(f"Audio codect not allowed: {codec_name}")
        if v is CodecType.data:
            return v
        assert_never(v)


class FFProbeFormat(BaseModel):
    """Relevant parts of the `format` section of a `ffprobe` output."""

    filename: Path
    nb_streams: int
    format_name: list[str]
    duration: float = nan
    size: int
    bit_rate: int

    @validator("format_name", pre=True)
    def split_str(cls, v: Any) -> Any:
        """Returned as a comma-separated string by `ffprobe` sometimes."""
        if isinstance(v, str):
            return [part.strip() for part in v.split(",")]
        return v

    @validator("format_name")
    def ensure_whitelisted_format(cls, v: list[str]) -> list[str]:
        """Ensures at least one of the formats is whitelisted in settings."""
        if any(fmt in settings.video.container_formats_allowed for fmt in v):
            return v
        raise ValueError(f"Container format not allowed: {v}")

    @property
    def names(self) -> list[str]:
        """Convenience property around poorly chosen ffprobe section name."""
        return self.format_name


class VideoInfo(BaseModel):
    """Model for relevant `ffprobe` output."""

    streams: list[FFProbeStream]
    format: FFProbeFormat

    @validator("streams")
    def ensure_video_stream_is_present(
        cls,
        v: list[FFProbeStream],
    ) -> list[FFProbeStream]:
        """Ensures at least one video stream is present; no audio is fine."""
        if any(stream.codec_type == CodecType.video for stream in v):
            return v
        raise ValueError("No video stream present")

    def get_consistent_file_ext(self, *, normalize: bool = True) -> str:
        """
        Return a file extension that is consistent with the video format.

        Without normalization the extension is first taken from the file name
        and checked against the format names for the video.
        If it matches one of them, it is returned.
        If not, one of them is arbitrarily picked and a corresponding file
        extension is returned. If for example `mov` is one of the names for
        the format, then `.mov` may be returned as a consistent extension.

        Args:
            normalize (optional):
                If `True` (default), the method tries to be more deterministic
                in choosing an extension, by searching for common format names
                for the video first and picking the extension accordingly:
                If `mp4` is one of the format names, `.mp4` will be returned;
                otherwise, if `webm` is one of the names, `.webm` is returned.
                If `False` the starting point will be the file name's given
                extension.

        Returns:
            The string representing a file extension for the video that is
            consistent with its format names; it will always start with a dot.
        """
        if normalize:
            if "mp4" in self.format.names:
                return ".mp4"
            if "webm" in self.format.names:
                return ".webm"
        # Fall back to file name:
        ext = self.format.filename.suffix
        # If the extension matches one of the format names, return it:
        if ext[1:] in self.format.names:
            return ext
        # Otherwise, infer an extension:
        ext = "." + self.format.names[0]
        _log.warning(
            f"File extension of video `{self.format.filename.name}` does not "
            f"match its formats {self.format.names}; choosing `{ext}` instead."
        )
        return ext

    def get_duration(self) -> float:
        """Get the duration of the longest stream in seconds."""
        max_duration = 0.
        if not isnan(self.format.duration):
            max_duration = self.format.duration
        for stream in self.streams:
            if not isnan(stream.duration):
                max_duration = max(stream.duration, max_duration)
        return max_duration
