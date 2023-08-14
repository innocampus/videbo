from enum import Enum
from math import isnan, nan
from pathlib import Path
from typing import Any, Optional
from typing_extensions import assert_never

from pydantic import BaseModel, validator

from videbo import settings
from videbo.exceptions import (
    AudioCodecNotAllowed,
    ContainerFormatNotAllowed,
    VideoCodecNotAllowed,
)


__all__ = [
    "FFProbeFormat",
    "FFProbeStream",
    "VideoInfo",
]


class CodecType(str, Enum):
    audio = 'audio'
    video = 'video'


class FFProbeStream(BaseModel):
    """Relevant parts of the `stream` section of a `ffprobe` output."""

    index: int
    codec_name: str
    codec_type: CodecType
    duration: float = nan

    def is_allowed(self) -> bool:
        """Ensures video/audio codec is whitelisted in settings."""
        if self.codec_type is CodecType.video:
            return self.codec_name in settings.video.video_codecs_allowed
        if self.codec_type is CodecType.audio:
            return self.codec_name in settings.video.audio_codecs_allowed
        assert_never(self.codec_type)


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

    @property
    def names(self) -> list[str]:
        """Convenience property around poorly chosen ffprobe section name."""
        return self.format_name

    def get_suggested_file_extension(self) -> str:
        """Get an appropriate file extension for the video format."""
        if "mp4" in self.names:
            return ".mp4"
        elif "webm" in self.names:
            return ".webm"
        else:
            raise ValueError(f"No recognized format among {self.names}")

    def is_allowed(self) -> bool:
        """Ensures at least one of the formats is whitelisted in settings."""
        return any(
            file_format in settings.video.container_formats_allowed
            for file_format in self.names
        )


class VideoInfo(BaseModel):
    """Model for relevant `ffprobe` output."""

    streams: list[FFProbeStream]
    format: FFProbeFormat
    file_ext: str = ""

    @validator("file_ext", always=True)
    def get_file_ext(cls, v: str, values: dict[str, Any]) -> str:
        """Ensure file extension is recognized and consistent with format."""
        fmt = values.get("format")
        assert isinstance(fmt, FFProbeFormat)
        if not v:
            return fmt.get_suggested_file_extension()
        if v.startswith(".") and v[1:] in fmt.names:
            return v
        elif not v.startswith(".") and v in fmt.names:
            return "." + v
        raise ValueError(f"{v} does not match formats {fmt.names}")

    def get_first_stream_of_type(
        self,
        codec: CodecType,
    ) -> Optional[FFProbeStream]:
        """Returns first stream of a certain type ("video" or "audio")."""
        for stream in self.streams:
            if stream.codec_type == codec:
                return stream
        return None

    def ensure_is_allowed(self) -> None:
        """
        Ensures format and video/audio codecs are whitelisted in settings.

        Allows videos with no audio stream.
        """
        if not self.format.is_allowed():
            raise ContainerFormatNotAllowed(self.format.names)
        video_stream = self.get_first_stream_of_type(CodecType.video)
        if video_stream is None:
            raise VideoCodecNotAllowed(None)
        if not video_stream.is_allowed():
            raise VideoCodecNotAllowed(video_stream.codec_name)
        audio_stream = self.get_first_stream_of_type(CodecType.audio)
        if audio_stream and not audio_stream.is_allowed():
            raise AudioCodecNotAllowed(audio_stream.codec_name)

    def get_duration(self) -> float:
        """Get the duration of the longest stream in seconds."""
        max_duration = 0.
        if not isnan(self.format.duration):
            max_duration = self.format.duration
        for stream in self.streams:
            if not isnan(stream.duration):
                max_duration = max(stream.duration, max_duration)
        return max_duration

    @staticmethod
    def content_type_for(file_ext: str) -> str:
        """Returns the right content-type for a given file extension."""
        if file_ext == ".mp4":
            return "video/mp4"
        if file_ext == ".webm":
            return "video/webm"
        raise ValueError(f"Unrecognized file extension `{file_ext}`")
