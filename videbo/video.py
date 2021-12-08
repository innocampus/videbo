import asyncio
import json
import shutil
from pathlib import Path
from typing import Dict, List, Optional

from videbo.settings import settings
from videbo.exceptions import (FileCmdError, FFMpegError, FFProbeError, InvalidMimeTypeError, InvalidVideoError,
                               UnknownProgramError)


class VideoConfig:
    def __init__(self, user: str = None, binary_file: str = None, binary_ffmpeg: str = None,
                 binary_ffprobe: str = None):
        self.user = user or settings.check_user
        self.binary_ffmpeg = binary_ffmpeg or settings.binary_ffmpeg
        self.binary_ffprobe = binary_ffprobe or settings.binary_ffprobe
        self.binary_file = binary_file or settings.binary_file

    async def create_sudo_subprocess(self, args: List[str], binary: str = None,
                                     stdout: int = asyncio.subprocess.DEVNULL,
                                     stderr: int = asyncio.subprocess.DEVNULL) -> asyncio.subprocess.Process:
        if binary == "file":
            program = self.binary_file
        elif binary == "ffprobe":
            program = self.binary_ffprobe
        else:
            raise UnknownProgramError()

        # Run as different user
        if self.user:
            program = "sudo"
            args = ["-u", self.user, binary] + args

        return await asyncio.create_subprocess_exec(program, *args, stdout=stdout, stderr=stderr)


class VideoInfo:
    """Wrapper for the ffprobe application to get information about a video file."""

    def __init__(self, video_file: Path, video_config: VideoConfig):
        self.video_file: Path = video_file
        self.valid_video = False
        self._video_config = video_config
        self.mime_type = None
        self.streams: List[Dict] = []
        self.format: Optional[Dict] = None

    async def fetch_mime_type(self) -> None:
        """Call file and fetch mime type."""

        args = ["-b", "-i", str(self.video_file)]
        # Run file command
        proc = await self._video_config.create_sudo_subprocess(args=args, binary="file", stdout=asyncio.subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), 10)
            self.mime_type = stdout.decode().strip().split(';')[0]  # strip linebreaks, extract first part
        except asyncio.TimeoutError:
            proc.kill()
            raise FileCmdError(True)

    async def fetch_info(self) -> None:
        """Call ffprobe and fetch information."""

        args = ["-show_format", "-show_streams", "-print_format", "json", str(self.video_file)]
        # Run ffprobe
        proc = await self._video_config.create_sudo_subprocess(args=args, binary="ffprobe",
                                                               stdout=asyncio.subprocess.PIPE,
                                                               stderr=asyncio.subprocess.PIPE)
        err = None
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), 10)
            err = stderr.decode()
            data = json.loads(stdout.decode())
            self.streams = data["streams"]
            self.format = data["format"]
        except asyncio.TimeoutError:
            proc.kill()
            raise FFProbeError(True, stderr=err)
        except KeyError:
            raise FFProbeError(False, stderr=err)

    def get_one_stream_type(self, codec_type: str) -> Optional[Dict]:
        """Get one stream in video of a certain type (video, audio)"""
        for stream in self.streams:
            if stream["codec_type"] == codec_type:
                return stream

        return None

    def get_length(self) -> float:
        """Get the length/duration of the video."""
        max_duration = 0.0
        # Get duration of longest stream
        for stream in self.streams:
            if "duration" in stream:
                duration = float(stream["duration"])
                if duration == duration:  # Skip NaNs
                    max_duration = max(duration, max_duration)

        if "duration" in self.format:
            duration = float(self.format["duration"])
            if duration == duration:  # Skip NaNs
                max_duration = max(duration, max_duration)

        return max_duration

    def get_file_format_container(self) -> List[str]:
        """Return the container formats as list."""
        formats = self.format.get("format_name", "")  # type: str
        return formats.split(',')

    def get_suggested_file_extension(self) -> str:
        """Get a reasonable file extension."""
        formats = self.get_file_format_container()
        if "mp4" in formats:
            return ".mp4"
        if "webm" in formats:
            return ".webm"

        raise InvalidVideoError()


class VideoValidator:
    def __init__(self, info: VideoInfo):
        self._info = info

    def check_valid_mime_type(self) -> Optional[bool]:
        """Check if this is a valid, whitelisted video file type."""
        if self._info.mime_type.lower() in settings.mime_types_allowed:
            return True
        raise InvalidMimeTypeError(self._info.mime_type)

    def check_valid_video(self) -> None:
        """Check if this a video whose video/audio codec and container format are on our whitelists."""
        valid_format = False
        valid_video = False
        video_codec = ''
        audio_codec = ''

        # Check container format, for some reasons ffprobe returns multiple format names.
        formats = self._info.get_file_format_container()
        for file_format in formats:
            if file_format in settings.container_formats_allowed:
                valid_format = True

        video_stream = self._info.get_one_stream_type("video")
        if video_stream:
            video_codec = video_stream["codec_name"]
            valid_video = video_codec in settings.video_codecs_allowed

        audio_stream = self._info.get_one_stream_type("audio")
        if audio_stream:
            audio_codec = audio_stream["codec_name"]
            valid_audio = audio_codec in settings.audio_codecs_allowed
        else:
            # Accept videos without an audio stream.
            valid_audio = True

        if not valid_format or not valid_video or not valid_audio:
            raise InvalidVideoError(','.join(formats), video_codec, audio_codec)


class Video:
    def __init__(self, video_config: VideoConfig):
        self.video_config = video_config

    async def save_thumbnail(self, video_in: Path, thumbnail_out: Path, offset: int, height: int,
                             temp_output_file: Optional[Path] = None) -> None:
        """Call ffmpeg and save scaled frame at specified offset."""
        output_file = thumbnail_out
        if temp_output_file is not None:
            output_file = temp_output_file

        args = ["-ss", str(offset), "-i", str(video_in), "-vframes", "1", "-an", "-vf",
                "scale=-1:{0}".format(height), "-y", str(output_file)]
        # Run ffmpeg
        if self.video_config.user:
            args = ["-u", self.video_config.user, self.video_config.binary_ffmpeg] + args
            binary = "sudo"
        else:
            binary = self.video_config.binary_ffmpeg
        proc = await asyncio.create_subprocess_exec(binary, *args, stdout=asyncio.subprocess.DEVNULL,
                                                    stderr=asyncio.subprocess.DEVNULL)
        try:
            await asyncio.wait_for(proc.wait(), 10)
            if proc.returncode != 0:
                raise FFMpegError(False)

            if output_file != thumbnail_out:
                # Copy temp file to target
                await asyncio.get_event_loop().run_in_executor(None, self._copy_file, output_file, thumbnail_out)

        except asyncio.TimeoutError:
            proc.kill()
            raise FFMpegError(True)

        finally:
            if output_file != thumbnail_out:
                # Delete temp file
                await asyncio.get_event_loop().run_in_executor(None, self._delete_file, output_file)

    @staticmethod
    def _delete_file(file_path: Path) -> bool:
        # Check source file really exists.
        if not file_path.is_file():
            return False

        file_path.unlink()
        return True

    @staticmethod
    def _copy_file(source_file: Path, target_file: Path) -> None:
        # Check source file really exists.
        if not source_file.is_file():
            raise FileNotFoundError()

        # Copy if destination doesn't exist
        if not target_file.is_file():
            shutil.copyfile(str(source_file), str(target_file))


def get_content_type_for_video(file_ext: str):
    if file_ext == ".mp4":
        return "video/mp4"
    if file_ext == ".webm":
        return "video/webm"
